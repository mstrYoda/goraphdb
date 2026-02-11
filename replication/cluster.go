package replication

import (
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"

	graphdb "github.com/mstrYoda/goraphdb"
	pb "github.com/mstrYoda/goraphdb/replication/proto"
	"google.golang.org/grpc"
)

// ---------------------------------------------------------------------------
// Cluster Manager — automatic cluster formation
//
// The ClusterManager wires together leader election (Raft), WAL replication
// (gRPC), and role management into a single, easy-to-use component. When
// the database is started in cluster mode, calling StartCluster sets up
// everything automatically:
//
//   - Starts a Raft node for leader election.
//   - When this node becomes the leader:
//     • DB role is set to "leader" (writes accepted)
//     • A gRPC replication server is started to serve WAL entries to followers
//   - When this node becomes a follower:
//     • DB role is set to "follower" (writes rejected)
//     • A gRPC replication client connects to the leader for WAL streaming
//
// Architecture:
//
//	                  ┌──────────────────────────────┐
//	                  │    ClusterManager.Start()     │
//	                  │                              │
//	                  │  1. Start Raft Election       │
//	                  │  2. Register OnRoleChange     │
//	                  └──────────┬───────────────────┘
//	                             │
//	              ┌──────────────┴──────────────┐
//	              ▼                             ▼
//	     ┌────────────────┐          ┌────────────────────┐
//	     │  becomeLeader  │          │  becomeFollower     │
//	     │                │          │                     │
//	     │ DB.SetRole     │          │ DB.SetRole          │
//	     │   ("leader")   │          │   ("follower")      │
//	     │                │          │                     │
//	     │ Start gRPC     │          │ Connect to leader   │
//	     │ repl server    │          │ gRPC repl client    │
//	     └────────────────┘          └────────────────────┘
//
// Limitations (v1):
//   - After leader failover, the new leader's WAL starts fresh. Followers
//     reset their applied LSN and only receive new writes from the new leader.
//     Historical data already on followers is preserved (idempotent applier).
//   - Epoch-based LSN for seamless failover is planned for a future version.
// ---------------------------------------------------------------------------

// ClusterConfig configures automatic cluster formation with leader election
// and WAL replication.
type ClusterConfig struct {
	// NodeID is a unique identifier for this node in the cluster.
	NodeID string
	// RaftBindAddr is the address to bind the Raft transport to (e.g. "0.0.0.0:7000").
	RaftBindAddr string
	// RaftAdvertiseAddr is the address advertised to other Raft nodes.
	// If empty, RaftBindAddr is used.
	RaftAdvertiseAddr string
	// RaftDataDir is the directory for Raft's internal state (log store, stable store).
	RaftDataDir string
	// GRPCAddr is the address this node listens on for gRPC WAL replication
	// when it is the leader (e.g. "0.0.0.0:7001").
	GRPCAddr string
	// HTTPAddr is this node's HTTP API address (e.g. "http://127.0.0.1:7474").
	// Used by followers to forward write operations to the leader.
	HTTPAddr string
	// Bootstrap is true if this node should bootstrap a new cluster.
	// All initial nodes should set this to true with the same peer list.
	Bootstrap bool
	// Peers is the set of other nodes in the cluster.
	Peers []ClusterPeer
	// Logger for cluster events.
	Logger *slog.Logger
}

// ClusterPeer represents another node in the cluster.
type ClusterPeer struct {
	// ID is the unique identifier for this peer.
	ID string
	// RaftAddr is the peer's Raft transport address (e.g. "10.0.1.2:7000").
	RaftAddr string
	// GRPCAddr is the peer's gRPC replication address (e.g. "10.0.1.2:7001").
	GRPCAddr string
	// HTTPAddr is the peer's HTTP API address (e.g. "http://10.0.1.2:7474").
	// Used for write forwarding from followers to the leader.
	HTTPAddr string
}

// ClusterManager orchestrates leader election, WAL replication, and role
// transitions for a database node in a cluster.
type ClusterManager struct {
	db       *graphdb.DB
	config   ClusterConfig
	log      *slog.Logger
	election *Election
	applier  *graphdb.Applier
	router   *Router

	mu         sync.Mutex
	grpcServer *grpc.Server
	grpcLis    net.Listener
	replClient *Client

	// Map of nodeID → gRPC address for leader discovery.
	peerGRPCAddrs map[string]string
	// Map of nodeID → HTTP address for write forwarding.
	peerHTTPAddrs map[string]string
}

// StartCluster initializes the cluster manager: starts Raft election, and
// automatically starts/stops gRPC replication server or client based on role.
//
// Requirements:
//   - The DB must be opened with EnableWAL: true.
//   - All cluster nodes should use the same ShardCount.
//
// The DB's role is initially set to "follower" (safe default that rejects
// writes until election completes). Once Raft elects a leader, the role
// is updated automatically via the OnRoleChange callback.
func StartCluster(db *graphdb.DB, cfg ClusterConfig) (*ClusterManager, error) {
	// Validate required fields.
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("cluster: NodeID is required")
	}
	if cfg.RaftBindAddr == "" {
		return nil, fmt.Errorf("cluster: RaftBindAddr is required")
	}
	if cfg.GRPCAddr == "" {
		return nil, fmt.Errorf("cluster: GRPCAddr is required")
	}
	if cfg.RaftDataDir == "" {
		return nil, fmt.Errorf("cluster: RaftDataDir is required")
	}
	if db.WAL() == nil {
		return nil, fmt.Errorf("cluster: WAL must be enabled (Options.EnableWAL = true)")
	}

	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}

	// Build peer address maps (self + peers).
	peerGRPCAddrs := make(map[string]string)
	peerHTTPAddrs := make(map[string]string)
	peerGRPCAddrs[cfg.NodeID] = cfg.GRPCAddr
	peerHTTPAddrs[cfg.NodeID] = cfg.HTTPAddr

	var raftPeers []PeerConfig
	for _, p := range cfg.Peers {
		peerGRPCAddrs[p.ID] = p.GRPCAddr
		peerHTTPAddrs[p.ID] = p.HTTPAddr
		// Skip self — the election bootstrap already adds the current node.
		if p.ID == cfg.NodeID {
			continue
		}
		raftPeers = append(raftPeers, PeerConfig{ID: p.ID, Addr: p.RaftAddr})
	}

	// Start in follower mode (safe default until election completes).
	db.SetRole("follower")

	// Create the query router. It will be updated with the leader's HTTP
	// address whenever the election callback fires.
	router := NewRouter(db,
		WithRouterLogger(log.With("component", "router")),
	)

	cm := &ClusterManager{
		db:            db,
		config:        cfg,
		log:           log,
		applier:       graphdb.NewApplier(db),
		router:        router,
		peerGRPCAddrs: peerGRPCAddrs,
		peerHTTPAddrs: peerHTTPAddrs,
	}

	// Start Raft election.
	advertiseAddr := cfg.RaftAdvertiseAddr
	if advertiseAddr == "" {
		advertiseAddr = cfg.RaftBindAddr
	}

	election, err := NewElection(ElectionConfig{
		NodeID:        cfg.NodeID,
		BindAddr:      cfg.RaftBindAddr,
		AdvertiseAddr: advertiseAddr,
		DataDir:       cfg.RaftDataDir,
		Bootstrap:     cfg.Bootstrap,
		Peers:         raftPeers,
		OnRoleChange:  cm.onRoleChange,
		Logger:        log,
	})
	if err != nil {
		return nil, fmt.Errorf("cluster: start election: %w", err)
	}
	cm.election = election

	log.Info("cluster started",
		"node_id", cfg.NodeID,
		"raft_addr", cfg.RaftBindAddr,
		"grpc_addr", cfg.GRPCAddr,
		"http_addr", cfg.HTTPAddr,
		"bootstrap", cfg.Bootstrap,
		"peers", len(cfg.Peers),
	)

	return cm, nil
}

// onRoleChange is the Raft callback triggered when leadership changes.
func (cm *ClusterManager) onRoleChange(isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if isLeader {
		cm.becomeLeader()
	} else {
		cm.becomeFollower()
	}
}

// becomeLeader transitions this node to the leader role.
func (cm *ClusterManager) becomeLeader() {
	cm.log.Info("cluster: transitioning to LEADER")

	// Stop replication client if running (was a follower before).
	if cm.replClient != nil {
		cm.replClient.Stop()
		cm.replClient = nil
	}

	// Switch DB role to leader (enables writes).
	cm.db.SetRole("leader")

	// Update router: this node IS the leader, so clear forwarding address
	// (writes will be handled locally).
	cm.router.SetLeaderHTTPAddr("")

	// Start gRPC replication server so followers can connect.
	if cm.grpcServer == nil {
		if err := cm.startGRPCServer(); err != nil {
			cm.log.Error("cluster: failed to start gRPC replication server", "error", err)
			return
		}
	}

	cm.log.Info("cluster: now serving as LEADER",
		"grpc_addr", cm.config.GRPCAddr,
	)
}

// becomeFollower transitions this node to the follower role.
func (cm *ClusterManager) becomeFollower() {
	cm.log.Info("cluster: transitioning to FOLLOWER")

	// Switch DB role to follower (rejects writes).
	cm.db.SetRole("follower")

	// Stop gRPC server if running (was leader before).
	cm.stopGRPCServer()

	// Stop existing replication client if any.
	if cm.replClient != nil {
		cm.replClient.Stop()
		cm.replClient = nil
	}

	// Discover the leader's addresses.
	leaderID := cm.election.LeaderID()
	leaderGRPC, ok := cm.peerGRPCAddrs[leaderID]
	if !ok || leaderGRPC == "" {
		cm.log.Warn("cluster: leader gRPC address unknown, will retry on next leadership change",
			"leader_id", leaderID,
		)
		return
	}

	// Update router with the leader's HTTP address for write forwarding.
	leaderHTTP := cm.peerHTTPAddrs[leaderID]
	cm.router.SetLeaderHTTPAddr(leaderHTTP)
	if leaderHTTP != "" {
		cm.log.Info("cluster: router updated with leader HTTP address",
			"leader_id", leaderID,
			"leader_http", leaderHTTP,
		)
	}

	// Reset applier LSN — new leader has a fresh WAL LSN space.
	cm.applier.ResetLSN()

	// Start replication client pointing to the new leader.
	cm.replClient = NewClient(
		leaderGRPC,
		cm.applier,
		WithFollowerID(cm.config.NodeID),
		WithClientLogger(cm.log),
	)
	cm.replClient.Start()

	cm.log.Info("cluster: now replicating as FOLLOWER",
		"leader_id", leaderID,
		"leader_grpc", leaderGRPC,
	)
}

// startGRPCServer creates and starts the gRPC replication server.
func (cm *ClusterManager) startGRPCServer() error {
	lis, err := net.Listen("tcp", cm.config.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen gRPC %s: %w", cm.config.GRPCAddr, err)
	}

	cm.grpcLis = lis
	cm.grpcServer = grpc.NewServer()

	replServer := NewServer(cm.db, cm.db.WAL(), WithLogger(cm.log))
	pb.RegisterReplicationServiceServer(cm.grpcServer, replServer)

	go func() {
		if err := cm.grpcServer.Serve(lis); err != nil {
			// Serve returns ErrServerStopped on graceful stop, which is expected.
			cm.log.Debug("cluster: gRPC server stopped", "error", err)
		}
	}()

	return nil
}

// stopGRPCServer stops the gRPC server.
// Uses Stop() (not GracefulStop()) because followers have long-running
// streaming RPCs that would block GracefulStop() indefinitely.
func (cm *ClusterManager) stopGRPCServer() {
	if cm.grpcServer != nil {
		cm.grpcServer.Stop()
		cm.grpcServer = nil
	}
	if cm.grpcLis != nil {
		cm.grpcLis.Close()
		cm.grpcLis = nil
	}
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

// Router returns the query router that handles read/write splitting
// and write forwarding to the leader.
func (cm *ClusterManager) Router() *Router {
	return cm.router
}

// Election returns the underlying Raft election module.
func (cm *ClusterManager) Election() *Election {
	return cm.election
}

// Applier returns the WAL applier used by the replication client.
func (cm *ClusterManager) Applier() *graphdb.Applier {
	return cm.applier
}

// NodeID returns this node's unique identifier.
func (cm *ClusterManager) NodeID() string {
	return cm.config.NodeID
}

// IsLeader returns true if this node is currently the Raft leader.
func (cm *ClusterManager) IsLeader() bool {
	return cm.election.IsLeader()
}

// LeaderID returns the ID of the current leader, or empty if unknown.
func (cm *ClusterManager) LeaderID() string {
	return cm.election.LeaderID()
}

// Peers returns the configured cluster peers.
func (cm *ClusterManager) Peers() []ClusterPeer {
	return cm.config.Peers
}

// HTTPAddr returns this node's HTTP address.
func (cm *ClusterManager) HTTPAddr() string {
	return cm.config.HTTPAddr
}

// GRPCAddr returns this node's gRPC address.
func (cm *ClusterManager) GRPCAddr() string {
	return cm.config.GRPCAddr
}

// RaftAddr returns this node's Raft address.
func (cm *ClusterManager) RaftAddr() string {
	return cm.config.RaftBindAddr
}

// Close shuts down the cluster manager, stopping the replication client,
// gRPC server, and Raft election.
func (cm *ClusterManager) Close() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Stop replication client.
	if cm.replClient != nil {
		cm.replClient.Stop()
		cm.replClient = nil
	}

	// Stop gRPC server.
	cm.stopGRPCServer()

	// Stop election.
	if cm.election != nil {
		cm.election.Close()
	}

	cm.log.Info("cluster stopped", "node_id", cm.config.NodeID)
	return nil
}

// ---------------------------------------------------------------------------
// Peer parsing helpers
// ---------------------------------------------------------------------------

// ParsePeers parses a comma-separated peer list. Two formats are supported:
//
// 3-part (without HTTP):
//
//	"id@raft_addr@grpc_addr"
//
// 4-part (with HTTP address for write forwarding):
//
//	"id@raft_addr@grpc_addr@http_addr"
//
// Examples:
//
//	"node2@10.0.1.2:7000@10.0.1.2:7001"
//	"node2@10.0.1.2:7000@10.0.1.2:7001@http://10.0.1.2:7474"
func ParsePeers(s string) ([]ClusterPeer, error) {
	if s == "" {
		return nil, nil
	}

	parts := strings.Split(s, ",")
	peers := make([]ClusterPeer, 0, len(parts))

	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}

		fields := strings.Split(p, "@")
		if len(fields) < 3 || len(fields) > 4 {
			return nil, fmt.Errorf("invalid peer format %q: expected id@raft_addr@grpc_addr[@http_addr]", p)
		}

		peer := ClusterPeer{
			ID:       fields[0],
			RaftAddr: fields[1],
			GRPCAddr: fields[2],
		}
		if len(fields) == 4 {
			peer.HTTPAddr = fields[3]
		}

		peers = append(peers, peer)
	}

	return peers, nil
}
