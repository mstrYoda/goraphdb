package main

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// OpType identifies the kind of operation being benchmarked.
// ---------------------------------------------------------------------------

type OpType string

const (
	OpCreateNode  OpType = "CreateNode"
	OpCreateEdge  OpType = "CreateEdge"
	OpMergeNode   OpType = "MergeNode"
	OpUpdateNode  OpType = "UpdateNode"
	OpPointRead   OpType = "PointRead"
	OpLabelScan   OpType = "LabelScan"
	OpTraversal   OpType = "Traversal"
	OpNeighborhood  OpType = "Neighborhood"
	OpIndexedLookup OpType = "IndexedLookup"
	OpCanaryWrite   OpType = "CanaryWrite"
	OpCanaryRead    OpType = "CanaryRead"
)

// ---------------------------------------------------------------------------
// Sample is a single recorded measurement.
// ---------------------------------------------------------------------------

type Sample struct {
	Op       OpType
	Duration time.Duration
	IsError  bool
}

// ---------------------------------------------------------------------------
// Histogram collects latency samples for a single operation type and computes
// percentiles using a sorted slice (no external dependency needed).
// ---------------------------------------------------------------------------

type Histogram struct {
	mu       sync.Mutex
	samples  []time.Duration
	errCount uint64
}

func (h *Histogram) Record(d time.Duration, isErr bool) {
	h.mu.Lock()
	h.samples = append(h.samples, d)
	if isErr {
		h.errCount++
	}
	h.mu.Unlock()
}

type HistStats struct {
	Count  int
	Errors int
	P50    time.Duration
	P95    time.Duration
	P99    time.Duration
	Max    time.Duration
	Min    time.Duration
	Avg    time.Duration
}

func (h *Histogram) Stats() HistStats {
	h.mu.Lock()
	// copy so we can sort without holding the lock long
	cp := make([]time.Duration, len(h.samples))
	copy(cp, h.samples)
	errs := h.errCount
	h.mu.Unlock()

	n := len(cp)
	if n == 0 {
		return HistStats{}
	}

	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })

	var sum time.Duration
	for _, d := range cp {
		sum += d
	}

	return HistStats{
		Count:  n,
		Errors: int(errs),
		P50:    cp[percentileIdx(n, 50)],
		P95:    cp[percentileIdx(n, 95)],
		P99:    cp[percentileIdx(n, 99)],
		Max:    cp[n-1],
		Min:    cp[0],
		Avg:    sum / time.Duration(n),
	}
}

func percentileIdx(n int, pct int) int {
	idx := int(math.Ceil(float64(pct)/100.0*float64(n))) - 1
	if idx < 0 {
		return 0
	}
	if idx >= n {
		return n - 1
	}
	return idx
}

// ---------------------------------------------------------------------------
// Collector aggregates samples from all workers via a channel.
// ---------------------------------------------------------------------------

type Collector struct {
	ch          chan Sample
	histograms  map[OpType]*Histogram
	totalOps    atomic.Int64
	totalErrors atomic.Int64

	// Replication lag samples (durations).
	repLagMu      sync.Mutex
	repLagSamples []time.Duration

	startTime time.Time
	done      chan struct{}
}

func NewCollector(bufSize int) *Collector {
	c := &Collector{
		ch:         make(chan Sample, bufSize),
		histograms: make(map[OpType]*Histogram),
		done:       make(chan struct{}),
	}
	// Pre-create histograms for known op types.
	for _, op := range []OpType{
		OpCreateNode, OpCreateEdge, OpMergeNode, OpUpdateNode,
		OpPointRead, OpLabelScan, OpTraversal, OpNeighborhood, OpIndexedLookup,
	} {
		c.histograms[op] = &Histogram{}
	}
	return c
}

// Record sends a sample to the collector (non-blocking if buffer has space).
func (c *Collector) Record(s Sample) {
	c.ch <- s
}

// RecordRepLag records a single replication lag measurement.
func (c *Collector) RecordRepLag(d time.Duration) {
	c.repLagMu.Lock()
	c.repLagSamples = append(c.repLagSamples, d)
	c.repLagMu.Unlock()
}

// Start begins the background goroutine that drains the sample channel.
func (c *Collector) Start() {
	c.startTime = time.Now()
	go func() {
		for s := range c.ch {
			h, ok := c.histograms[s.Op]
			if !ok {
				h = &Histogram{}
				c.histograms[s.Op] = h
			}
			h.Record(s.Duration, s.IsError)
			c.totalOps.Add(1)
			if s.IsError {
				c.totalErrors.Add(1)
			}
		}
		close(c.done)
	}()
}

// Stop closes the channel and waits for the drain goroutine to finish.
func (c *Collector) Stop() {
	close(c.ch)
	<-c.done
}

// ---------------------------------------------------------------------------
// Live progress line (called every tick from main).
// ---------------------------------------------------------------------------

func (c *Collector) ProgressLine(elapsed time.Duration) string {
	secs := elapsed.Seconds()
	if secs < 0.001 {
		secs = 0.001
	}

	// Aggregate write and read ops.
	var writeOps, readOps int
	var writeP99, readP99 time.Duration

	writeTypes := []OpType{OpCreateNode, OpCreateEdge, OpMergeNode, OpUpdateNode}
	readTypes := []OpType{OpPointRead, OpLabelScan, OpTraversal, OpNeighborhood, OpIndexedLookup}

	for _, op := range writeTypes {
		if h, ok := c.histograms[op]; ok {
			st := h.Stats()
			writeOps += st.Count
			if st.P99 > writeP99 {
				writeP99 = st.P99
			}
		}
	}
	for _, op := range readTypes {
		if h, ok := c.histograms[op]; ok {
			st := h.Stats()
			readOps += st.Count
			if st.P99 > readP99 {
				readP99 = st.P99
			}
		}
	}

	errRate := float64(c.totalErrors.Load()) / math.Max(float64(c.totalOps.Load()), 1) * 100

	return fmt.Sprintf("[%3.0fs] W: %6.0f ops/s (p99: %5s)  R: %6.0f ops/s (p99: %5s)  Err: %.1f%%",
		secs,
		float64(writeOps)/secs, fmtDur(writeP99),
		float64(readOps)/secs, fmtDur(readP99),
		errRate,
	)
}

// ---------------------------------------------------------------------------
// Final report.
// ---------------------------------------------------------------------------

func (c *Collector) PrintReport(elapsed time.Duration, cfg BenchConfig) {
	secs := elapsed.Seconds()

	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println("  GoraphDB Load Test Results")
	fmt.Printf("  Duration: %s | Writers: %d | Readers: %d\n", elapsed.Round(time.Second), cfg.Writers, cfg.Readers)
	fmt.Printf("  Targets: %d node(s)\n", len(cfg.Targets))
	fmt.Println("═══════════════════════════════════════════════════════════════")

	// Aggregate writes.
	writeTypes := []OpType{OpCreateNode, OpCreateEdge, OpMergeNode, OpUpdateNode}
	readTypes := []OpType{OpPointRead, OpLabelScan, OpTraversal, OpNeighborhood}

	printCategory := func(label string, ops []OpType) {
		var total, errors int
		var allDurations []time.Duration
		for _, op := range ops {
			if h, ok := c.histograms[op]; ok {
				st := h.Stats()
				total += st.Count
				errors += st.Errors
				h.mu.Lock()
				allDurations = append(allDurations, h.samples...)
				h.mu.Unlock()
			}
		}
		if total == 0 {
			fmt.Printf("\n  %s\n    (no operations recorded)\n", label)
			return
		}
		sort.Slice(allDurations, func(i, j int) bool { return allDurations[i] < allDurations[j] })
		n := len(allDurations)
		var sum time.Duration
		for _, d := range allDurations {
			sum += d
		}

		errPct := float64(errors) / float64(total) * 100
		fmt.Printf("\n  %s\n", label)
		fmt.Printf("    Total ops     : %s\n", fmtInt(total))
		fmt.Printf("    Throughput    : %s ops/s\n", fmtInt(int(float64(total)/secs)))
		fmt.Printf("    Latency p50   : %s\n", fmtDur(allDurations[percentileIdx(n, 50)]))
		fmt.Printf("    Latency p95   : %s\n", fmtDur(allDurations[percentileIdx(n, 95)]))
		fmt.Printf("    Latency p99   : %s\n", fmtDur(allDurations[percentileIdx(n, 99)]))
		fmt.Printf("    Latency max   : %s\n", fmtDur(allDurations[n-1]))
		fmt.Printf("    Latency avg   : %s\n", fmtDur(sum/time.Duration(n)))
		fmt.Printf("    Errors        : %s (%.3f%%)\n", fmtInt(errors), errPct)
	}

	printCategory("WRITES", writeTypes)
	printCategory("READS", readTypes)

	// Replication lag.
	c.repLagMu.Lock()
	lagCp := make([]time.Duration, len(c.repLagSamples))
	copy(lagCp, c.repLagSamples)
	c.repLagMu.Unlock()

	if len(lagCp) > 0 {
		sort.Slice(lagCp, func(i, j int) bool { return lagCp[i] < lagCp[j] })
		n := len(lagCp)
		fmt.Printf("\n  REPLICATION LAG (%d samples)\n", n)
		fmt.Printf("    Lag p50       : %s\n", fmtDur(lagCp[percentileIdx(n, 50)]))
		fmt.Printf("    Lag p95       : %s\n", fmtDur(lagCp[percentileIdx(n, 95)]))
		fmt.Printf("    Lag p99       : %s\n", fmtDur(lagCp[percentileIdx(n, 99)]))
		fmt.Printf("    Lag max       : %s\n", fmtDur(lagCp[n-1]))
	}

	// Per-operation breakdown.
	fmt.Println("\n  BREAKDOWN BY OPERATION")
	allOps := append(writeTypes, readTypes...)
	for _, op := range allOps {
		if h, ok := c.histograms[op]; ok {
			st := h.Stats()
			if st.Count == 0 {
				continue
			}
			fmt.Printf("    %-14s: %8s ops  (%s p50, %s p99)\n",
				string(op), fmtInt(st.Count), fmtDur(st.P50), fmtDur(st.P99))
		}
	}

	fmt.Println("\n═══════════════════════════════════════════════════════════════")
}

// ---------------------------------------------------------------------------
// Format helpers.
// ---------------------------------------------------------------------------

func fmtDur(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%.0fus", float64(d.Microseconds()))
	}
	if d < time.Second {
		return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000.0)
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
}

func fmtInt(n int) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	if n < 1_000_000 {
		return fmt.Sprintf("%d,%03d", n/1000, n%1000)
	}
	return fmt.Sprintf("%d,%03d,%03d", n/1_000_000, (n/1000)%1000, n%1000)
}
