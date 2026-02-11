// Command graphdb-bench is a load testing tool for GoraphDB clusters.
//
// It sends concurrent read and write HTTP requests against one or more
// GoraphDB nodes, measuring throughput, latency percentiles, error rates,
// and replication lag.
//
// Usage:
//
//	go run ./cmd/graphdb-bench/ -targets http://127.0.0.1:7474 -duration 30s -writers 8 -readers 16
//
// For cluster benchmarks, list all nodes:
//
//	go run ./cmd/graphdb-bench/ \
//	  -targets "http://127.0.0.1:7474,http://127.0.0.1:7475,http://127.0.0.1:7476" \
//	  -duration 60s -writers 16 -readers 32
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

// BenchConfig holds all CLI-driven configuration for the benchmark.
type BenchConfig struct {
	Targets   []string
	Duration  time.Duration
	Writers   int
	Readers   int
	SeedNodes int
	SeedEdges int
	Warmup    time.Duration
	Cooldown  time.Duration
	RepProbe  bool // whether to run replication lag probing
}

func main() {
	var (
		targetsStr = flag.String("targets", "http://127.0.0.1:7474", "Comma-separated list of GoraphDB HTTP endpoints")
		duration   = flag.Duration("duration", 30*time.Second, "Benchmark duration (e.g. 30s, 1m, 5m)")
		writers    = flag.Int("writers", 8, "Number of concurrent write goroutines")
		readers    = flag.Int("readers", 16, "Number of concurrent read goroutines")
		seedNodeCount = flag.Int("seed-nodes", 200, "Number of nodes to create during warmup")
		seedEdgeCount = flag.Int("seed-edges", 100, "Number of edges to create during warmup")
		warmup     = flag.Duration("warmup", 5*time.Second, "Warmup duration before collecting stats")
		cooldown   = flag.Duration("cooldown", 2*time.Second, "Cooldown duration after stopping writers")
		noRepProbe = flag.Bool("no-rep-probe", false, "Disable replication lag probing")
	)
	flag.Parse()

	targets := strings.Split(*targetsStr, ",")
	for i := range targets {
		targets[i] = strings.TrimSpace(targets[i])
	}

	cfg := BenchConfig{
		Targets:   targets,
		Duration:  *duration,
		Writers:   *writers,
		Readers:   *readers,
		SeedNodes: *seedNodeCount,
		SeedEdges: *seedEdgeCount,
		Warmup:    *warmup,
		Cooldown:  *cooldown,
		RepProbe:  !*noRepProbe,
	}

	printBanner(cfg)

	// -----------------------------------------------------------------------
	// Step 1: Health check all targets.
	// -----------------------------------------------------------------------
	fmt.Print("Checking targets... ")
	if err := checkTargets(cfg.Targets); err != nil {
		fmt.Fprintf(os.Stderr, "\nERROR: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("OK")

	// -----------------------------------------------------------------------
	// Step 2: Seed data.
	// -----------------------------------------------------------------------
	registry := &NodeRegistry{}
	fmt.Printf("Seeding %d nodes + %d edges... ", cfg.SeedNodes, cfg.SeedEdges)

	sCtx, sCancel := context.WithTimeout(context.Background(), 60*time.Second)
	if err := seedNodes(sCtx, cfg.Targets[0], cfg.SeedNodes, registry); err != nil {
		sCancel()
		fmt.Fprintf(os.Stderr, "\nERROR seeding nodes: %v\n", err)
		os.Exit(1)
	}
	if err := seedEdges(sCtx, cfg.Targets[0], cfg.SeedEdges, registry); err != nil {
		sCancel()
		fmt.Fprintf(os.Stderr, "\nERROR seeding edges: %v\n", err)
		os.Exit(1)
	}
	sCancel()
	fmt.Printf("OK (%d nodes registered)\n", registry.Len())

	// -----------------------------------------------------------------------
	// Step 3: Warmup phase — run workers but don't collect stats.
	// -----------------------------------------------------------------------
	collector := NewCollector(100_000)
	collector.Start()

	rotator := NewTargetRotator(cfg.Targets)

	// Handle Ctrl+C gracefully.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	if cfg.Warmup > 0 {
		fmt.Printf("Warming up for %s...\n", cfg.Warmup)
		warmupCtx, warmupCancel := context.WithTimeout(context.Background(), cfg.Warmup)

		var warmupWg sync.WaitGroup
		for i := 0; i < cfg.Writers; i++ {
			warmupWg.Add(1)
			go func(id int) {
				defer warmupWg.Done()
				runWriteWorker(warmupCtx, id, rotator, registry, collector, true)
			}(i)
		}

		select {
		case <-warmupCtx.Done():
		case <-sigCh:
			warmupCancel()
			fmt.Println("\nInterrupted during warmup.")
			os.Exit(0)
		}
		warmupCancel()
		warmupWg.Wait()
		fmt.Printf("Warmup done. Registry has %d nodes.\n", registry.Len())
	}

	// -----------------------------------------------------------------------
	// Step 4: Steady-state benchmark.
	// -----------------------------------------------------------------------
	// Reset collector for clean stats (create a new one).
	collector.Stop()
	collector = NewCollector(500_000)
	collector.Start()

	fmt.Printf("\nRunning benchmark for %s with %d writers + %d readers...\n\n", cfg.Duration, cfg.Writers, cfg.Readers)

	benchCtx, benchCancel := context.WithTimeout(context.Background(), cfg.Duration)
	benchStart := time.Now()

	var benchWg sync.WaitGroup

	// Start write workers.
	for i := 0; i < cfg.Writers; i++ {
		benchWg.Add(1)
		go func(id int) {
			defer benchWg.Done()
			runWriteWorker(benchCtx, id, rotator, registry, collector, false)
		}(i)
	}

	// Start read workers.
	for i := 0; i < cfg.Readers; i++ {
		benchWg.Add(1)
		go func(id int) {
			defer benchWg.Done()
			runReadWorker(benchCtx, id, rotator, registry, collector)
		}(i)
	}

	// Start replication lag probe (if enabled and multiple targets).
	if cfg.RepProbe && len(cfg.Targets) > 1 {
		benchWg.Add(1)
		go func() {
			defer benchWg.Done()
			runReplicationProbe(benchCtx, cfg.Targets, collector, 2*time.Second)
		}()
	}

	// Progress ticker.
	progressTicker := time.NewTicker(1 * time.Second)
	defer progressTicker.Stop()

	go func() {
		for {
			select {
			case <-benchCtx.Done():
				return
			case <-progressTicker.C:
				elapsed := time.Since(benchStart)
				fmt.Printf("\r%s", collector.ProgressLine(elapsed))
			}
		}
	}()

	// Wait for either duration or Ctrl+C.
	select {
	case <-benchCtx.Done():
	case <-sigCh:
		fmt.Println("\n\nInterrupted! Stopping benchmark...")
		benchCancel()
	}

	benchCancel()
	benchWg.Wait()
	elapsed := time.Since(benchStart)

	// -----------------------------------------------------------------------
	// Step 5: Cooldown.
	// -----------------------------------------------------------------------
	if cfg.Cooldown > 0 {
		fmt.Printf("\n\nCooling down for %s...", cfg.Cooldown)
		time.Sleep(cfg.Cooldown)
		fmt.Println(" done")
	}

	// -----------------------------------------------------------------------
	// Step 6: Report.
	// -----------------------------------------------------------------------
	collector.Stop()
	collector.PrintReport(elapsed, cfg)
}

func printBanner(cfg BenchConfig) {
	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║            GoraphDB Load Test (graphdb-bench)            ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Printf("  Targets  : %s\n", strings.Join(cfg.Targets, ", "))
	fmt.Printf("  Duration : %s\n", cfg.Duration)
	fmt.Printf("  Writers  : %d goroutines\n", cfg.Writers)
	fmt.Printf("  Readers  : %d goroutines\n", cfg.Readers)
	fmt.Printf("  Seed     : %d nodes, %d edges\n", cfg.SeedNodes, cfg.SeedEdges)
	fmt.Printf("  Warmup   : %s\n", cfg.Warmup)
	fmt.Printf("  RepProbe : %v\n", cfg.RepProbe)
	fmt.Println()
}
