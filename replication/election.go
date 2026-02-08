package replication

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// ---------------------------------------------------------------------------
// Leader Election — hashicorp/raft integration
//
// This module uses hashicorp/raft ONLY for leader election and cluster
// membership management. Data replication is handled separately by the
// WAL + gRPC log shipping pipeline (Phases 1–3).
//
// Architecture:
//
//	┌─────────────────────────────────────────────────────┐
//	│  Raft Cluster (3 or 5 nodes)                        │
//	│                                                     │
//	│  Node A ◄──── heartbeats ────► Node B               │
//	│  (Leader)                      (Follower)           │
//	│    │                             │                  │
//	│    │ Raft log: only no-op        │ Raft log: only   │
//	│    │ barrier entries             │ no-op entries     │
//	│    │                             │                  │
//	│    ▼                             ▼                  │
//	│  Writes → WAL → gRPC ──────► Applier               │
//	│  (own pipeline)              (own pipeline)         │
//	└─────────────────────────────────────────────────────┘
//
// Why not use Raft for data replication too?
//   - Our WAL + streaming pipeline is already built and optimized for
//     graph mutations with msgpack encoding.
//   - Raft consensus adds latency to every write (majority ack required).
//   - For a read-replica pattern, we only need leader election — not
//     strong consistency on every write.
//   - If the leader crashes, the new leader's WAL is the source of truth.
//     Followers re-sync from the new leader's WAL after election.
//
// The FSM is intentionally minimal — it doesn't apply any real state.
// Raft is used purely for its leader election protocol.
// ---------------------------------------------------------------------------

// RoleChangeCallback is called when this node's role changes.
// isLeader is true when this node becomes the leader.
type RoleChangeCallback func(isLeader bool)

// ElectionConfig configures the Raft-based leader election.
type ElectionConfig struct {
	// NodeID is a unique identifier for this node in the cluster.
	NodeID string
	// BindAddr is the address to bind the Raft transport to (e.g. "0.0.0.0:7000").
	BindAddr string
	// AdvertiseAddr is the address advertised to other nodes (e.g. "10.0.1.1:7000").
	// If empty, BindAddr is used.
	AdvertiseAddr string
	// DataDir is the directory for Raft's internal state (log store, stable store).
	DataDir string
	// Bootstrap is true if this is the first node forming a new cluster.
	// Only one node should bootstrap; others join via AddVoter.
	Bootstrap bool
	// Peers is the initial set of peer addresses for bootstrapping.
	// Only used when Bootstrap is true. Each entry is "nodeID:addr".
	Peers []PeerConfig
	// OnRoleChange is called when this node's leadership status changes.
	OnRoleChange RoleChangeCallback
	// Logger for election events.
	Logger *slog.Logger
}

// PeerConfig represents a node in the Raft cluster.
type PeerConfig struct {
	ID   string
	Addr string
}

// Election manages leader election via hashicorp/raft.
type Election struct {
	raft         *raft.Raft
	config       ElectionConfig
	log          *slog.Logger
	transport    *raft.NetworkTransport
	logStore     *raftboltdb.BoltStore
	stableStore  *raftboltdb.BoltStore
	snapshotStore raft.SnapshotStore
	observerCh   chan raft.Observation
	stopCh       chan struct{}
}

// NewElection creates and starts a Raft node for leader election.
func NewElection(cfg ElectionConfig) (*Election, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.AdvertiseAddr == "" {
		cfg.AdvertiseAddr = cfg.BindAddr
	}

	// Ensure data directory exists.
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("election: create data dir: %w", err)
	}

	// Raft configuration.
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)
	// Tune for faster election in small clusters.
	raftConfig.HeartbeatTimeout = 1000 * time.Millisecond
	raftConfig.ElectionTimeout = 1000 * time.Millisecond
	raftConfig.LeaderLeaseTimeout = 500 * time.Millisecond
	raftConfig.CommitTimeout = 50 * time.Millisecond

	// Transport.
	addr, err := net.ResolveTCPAddr("tcp", cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("election: resolve bind addr: %w", err)
	}
	transport, err := raft.NewTCPTransport(cfg.BindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("election: create transport: %w", err)
	}

	// Log store and stable store (both use BoltDB).
	logStorePath := filepath.Join(cfg.DataDir, "raft-log.db")
	logStore, err := raftboltdb.NewBoltStore(logStorePath)
	if err != nil {
		return nil, fmt.Errorf("election: create log store: %w", err)
	}

	stableStorePath := filepath.Join(cfg.DataDir, "raft-stable.db")
	stableStore, err := raftboltdb.NewBoltStore(stableStorePath)
	if err != nil {
		logStore.Close()
		return nil, fmt.Errorf("election: create stable store: %w", err)
	}

	// Snapshot store (in-memory — we don't snapshot real state).
	snapshotStore := raft.NewDiscardSnapshotStore()

	// Create the Raft node.
	fsm := &electionFSM{}
	r, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		logStore.Close()
		stableStore.Close()
		return nil, fmt.Errorf("election: create raft: %w", err)
	}

	e := &Election{
		raft:          r,
		config:        cfg,
		log:           cfg.Logger,
		transport:     transport,
		logStore:      logStore,
		stableStore:   stableStore,
		snapshotStore: snapshotStore,
		observerCh:    make(chan raft.Observation, 16),
		stopCh:        make(chan struct{}),
	}

	// Bootstrap if this is the initial node.
	if cfg.Bootstrap {
		servers := []raft.Server{
			{
				ID:      raft.ServerID(cfg.NodeID),
				Address: raft.ServerAddress(cfg.AdvertiseAddr),
			},
		}
		for _, p := range cfg.Peers {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(p.ID),
				Address: raft.ServerAddress(p.Addr),
			})
		}
		future := r.BootstrapCluster(raft.Configuration{Servers: servers})
		if err := future.Error(); err != nil {
			// ErrCantBootstrap is OK — means already bootstrapped.
			if err != raft.ErrCantBootstrap {
				e.Close()
				return nil, fmt.Errorf("election: bootstrap: %w", err)
			}
		}
	}

	// Start observing leadership changes.
	observer := raft.NewObserver(e.observerCh, false, func(o *raft.Observation) bool {
		_, ok := o.Data.(raft.LeaderObservation)
		return ok
	})
	r.RegisterObserver(observer)
	go e.watchLeadership()

	cfg.Logger.Info("election started",
		"node_id", cfg.NodeID,
		"bind_addr", cfg.BindAddr,
		"bootstrap", cfg.Bootstrap,
	)

	return e, nil
}

// IsLeader returns true if this node is currently the Raft leader.
func (e *Election) IsLeader() bool {
	return e.raft.State() == raft.Leader
}

// LeaderAddr returns the address of the current leader, or empty if unknown.
func (e *Election) LeaderAddr() string {
	addr, _ := e.raft.LeaderWithID()
	return string(addr)
}

// LeaderID returns the ID of the current leader, or empty if unknown.
func (e *Election) LeaderID() string {
	_, id := e.raft.LeaderWithID()
	return string(id)
}

// AddVoter adds a new node to the Raft cluster as a voting member.
// Only the leader can add voters.
func (e *Election) AddVoter(id, addr string) error {
	future := e.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 5*time.Second)
	return future.Error()
}

// RemoveServer removes a node from the Raft cluster.
// Only the leader can remove servers.
func (e *Election) RemoveServer(id string) error {
	future := e.raft.RemoveServer(raft.ServerID(id), 0, 5*time.Second)
	return future.Error()
}

// Close shuts down the Raft node and cleans up resources.
func (e *Election) Close() error {
	close(e.stopCh)

	future := e.raft.Shutdown()
	if err := future.Error(); err != nil {
		e.log.Error("raft shutdown error", "error", err)
	}

	if e.logStore != nil {
		e.logStore.Close()
	}
	if e.stableStore != nil {
		e.stableStore.Close()
	}
	if e.transport != nil {
		e.transport.Close()
	}

	e.log.Info("election stopped", "node_id", e.config.NodeID)
	return nil
}

// watchLeadership monitors Raft leader observations and triggers callbacks.
func (e *Election) watchLeadership() {
	var wasLeader bool
	for {
		select {
		case <-e.stopCh:
			return
		case obs := <-e.observerCh:
			leaderObs, ok := obs.Data.(raft.LeaderObservation)
			if !ok {
				continue
			}

			isLeader := string(leaderObs.LeaderID) == e.config.NodeID
			if isLeader != wasLeader {
				wasLeader = isLeader
				if isLeader {
					e.log.Info("this node is now the LEADER", "node_id", e.config.NodeID)
				} else {
					e.log.Info("this node is now a FOLLOWER",
						"node_id", e.config.NodeID,
						"leader_id", string(leaderObs.LeaderID),
					)
				}
				if e.config.OnRoleChange != nil {
					e.config.OnRoleChange(isLeader)
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Minimal FSM — no real state, just satisfies the raft.FSM interface.
// ---------------------------------------------------------------------------

type electionFSM struct{}

func (f *electionFSM) Apply(l *raft.Log) interface{} {
	return nil
}

func (f *electionFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &electionSnapshot{}, nil
}

func (f *electionFSM) Restore(rc io.ReadCloser) error {
	return rc.Close()
}

type electionSnapshot struct{}

func (s *electionSnapshot) Persist(sink raft.SnapshotSink) error {
	return sink.Close()
}

func (s *electionSnapshot) Release() {}
