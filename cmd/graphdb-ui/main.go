// Command graphdb-ui starts the GraphDB management UI server.
//
// Usage:
//
//	go run ./cmd/graphdb-ui -db ./mydata.db
//	go run ./cmd/graphdb-ui -db ./mydata.db -addr :7474 -ui ./ui/dist
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	graphdb "github.com/mstrYoda/graphdb"
	"github.com/mstrYoda/graphdb/server"
)

func main() {
	dbPath := flag.String("db", "./data.db", "Path to GraphDB database directory")
	addr := flag.String("addr", ":7474", "HTTP listen address")
	uiDir := flag.String("ui", "", "Path to built UI static files (ui/dist)")
	shards := flag.Int("shards", 1, "Number of database shards")
	flag.Parse()

	opts := graphdb.DefaultOptions()
	opts.ShardCount = *shards

	db, err := graphdb.Open(*dbPath, opts)
	if err != nil {
		log.Fatalf("failed to open database at %s: %v", *dbPath, err)
	}
	defer db.Close()

	srv := server.New(db, *uiDir)

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
	fmt.Println()

	if err := http.ListenAndServe(*addr, srv); err != nil {
		log.Fatal(err)
	}
}
