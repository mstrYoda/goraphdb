// Command graphdb-ui starts the GraphDB management UI server.
//
// Standalone mode (default):
//
//	go run ./cmd/graphdb-ui -db ./mydata.db
//	go run ./cmd/graphdb-ui -db ./mydata.db -addr :7474 -ui ./ui/dist
//
// Cluster mode (3-node example):
//
//	# Node 1 (bootstrap):
//	go run ./cmd/graphdb-ui -db ./data1 -addr :7474 \
//	  -node-id node1 -raft-addr 127.0.0.1:7000 -grpc-addr 127.0.0.1:7001 \
//	  -bootstrap -peers "node2@127.0.0.1:7100@127.0.0.1:7101,node3@127.0.0.1:7200@127.0.0.1:7201"
//
//	# Node 2:
//	go run ./cmd/graphdb-ui -db ./data2 -addr :7475 \
//	  -node-id node2 -raft-addr 127.0.0.1:7100 -grpc-addr 127.0.0.1:7101 \
//	  -bootstrap -peers "node1@127.0.0.1:7000@127.0.0.1:7001,node3@127.0.0.1:7200@127.0.0.1:7201"
//
//	# Node 3:
//	go run ./cmd/graphdb-ui -db ./data3 -addr :7476 \
//	  -node-id node3 -raft-addr 127.0.0.1:7200 -grpc-addr 127.0.0.1:7201 \
//	  -bootstrap -peers "node1@127.0.0.1:7000@127.0.0.1:7001,node2@127.0.0.1:7100@127.0.0.1:7101"
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"path/filepath"

	graphdb "github.com/mstrYoda/goraphdb"
	"github.com/mstrYoda/goraphdb/replication"
	"github.com/mstrYoda/goraphdb/server"
)

func main() {
	// --- Database flags ---
	dbPath := flag.String("db", "./data.db", "Path to GraphDB database directory")
	addr := flag.String("addr", ":7474", "HTTP listen address")
	uiDir := flag.String("ui", "", "Path to built UI static files (ui/dist)")
	shards := flag.Int("shards", 1, "Number of database shards")

	// --- Cluster flags ---
	nodeID := flag.String("node-id", "", "Node ID for cluster mode (empty = standalone)")
	raftAddr := flag.String("raft-addr", "", "Raft transport bind address (e.g. 127.0.0.1:7000)")
	grpcAddr := flag.String("grpc-addr", "", "gRPC replication listen address (e.g. 127.0.0.1:7001)")
	bootstrap := flag.Bool("bootstrap", false, "Bootstrap a new Raft cluster")
	peers := flag.String("peers", "", `Comma-separated peer list: "id@raft_addr@grpc_addr[@http_addr],..."`)

	flag.Parse()

	clusterMode := *nodeID != ""

	opts := graphdb.DefaultOptions()
	opts.ShardCount = *shards

	// In cluster mode, enable WAL for replication.
	if clusterMode {
		opts.EnableWAL = true
	}

	db, err := graphdb.Open(*dbPath, opts)
	if err != nil {
		log.Fatalf("failed to open database at %s: %v", *dbPath, err)
	}
	defer db.Close()

	// Start cluster if configured.
	var cluster *replication.ClusterManager
	if clusterMode {
		if *raftAddr == "" {
			log.Fatal("-raft-addr is required in cluster mode")
		}
		if *grpcAddr == "" {
			log.Fatal("-grpc-addr is required in cluster mode")
		}

		clusterPeers, err := replication.ParsePeers(*peers)
		if err != nil {
			log.Fatalf("invalid -peers: %v", err)
		}

		// Construct this node's HTTP address for the cluster.
		// The addr flag is the HTTP listen address (e.g. ":7474").
		httpBase := "http://127.0.0.1" + *addr
		if (*addr)[0] != ':' {
			httpBase = "http://" + *addr
		}

		cluster, err = replication.StartCluster(db, replication.ClusterConfig{
			NodeID:       *nodeID,
			RaftBindAddr: *raftAddr,
			GRPCAddr:     *grpcAddr,
			HTTPAddr:     httpBase,
			RaftDataDir:  filepath.Join(*dbPath, "raft"),
			Bootstrap:    *bootstrap,
			Peers:        clusterPeers,
		})
		if err != nil {
			log.Fatalf("failed to start cluster: %v", err)
		}
		defer cluster.Close()
	}

	srv := server.New(db, *uiDir)

	// Wire the cluster router into the HTTP server for transparent
	// write forwarding. Followers will forward writes to the leader
	// via the Router; the leader executes them locally.
	if cluster != nil {
		srv.SetRouter(cluster.Router())
	}

	fmt.Println("╔══════════════════════════════════════╗")
	fmt.Println("║       GraphDB Management UI          ║")
	fmt.Println("╚══════════════════════════════════════╝")
	fmt.Printf("  Database : %s (%d shard(s))\n", *dbPath, *shards)
	fmt.Printf("  API      : http://localhost%s/api/\n", *addr)
	if *uiDir != "" {
		fmt.Printf("  UI       : http://localhost%s/\n", *addr)
	} else {
		fmt.Println("  UI       : not configured (use -ui ./ui/dist)")
		fmt.Println("  Dev mode : cd ui && npm run dev  (proxied to API)")
	}
	if clusterMode {
		fmt.Printf("  Cluster  : node=%s raft=%s grpc=%s bootstrap=%v\n",
			*nodeID, *raftAddr, *grpcAddr, *bootstrap)
		fmt.Printf("  Peers    : %d node(s)\n", len(*peers))
	} else {
		fmt.Println("  Mode     : standalone")
	}
	fmt.Println()

	if err := http.ListenAndServe(*addr, srv); err != nil {
		log.Fatal(err)
	}
}
