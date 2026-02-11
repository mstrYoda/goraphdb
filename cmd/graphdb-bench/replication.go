package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// ---------------------------------------------------------------------------
// Replication lag measurement.
//
// Approach: write a "canary" node to the leader (first target), then poll
// each follower (remaining targets) until it appears. The time between the
// write and the successful read on each follower is recorded as the
// replication lag for that sample.
// ---------------------------------------------------------------------------

func runReplicationProbe(
	ctx context.Context,
	targets []string,
	collector *Collector,
	interval time.Duration,
) {
	if len(targets) < 2 {
		// Only one target — nothing to measure.
		return
	}

	leader := targets[0]
	followers := targets[1:]
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	seq := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			seq++
			probeOnce(ctx, leader, followers, collector, seq)
		}
	}
}

func probeOnce(
	ctx context.Context,
	leader string,
	followers []string,
	collector *Collector,
	seq int,
) {
	marker := fmt.Sprintf("canary_%d_%d", time.Now().UnixNano(), seq)

	// Step 1: Write canary to leader.
	query := fmt.Sprintf(
		`CREATE (n:_Canary {marker: "%s", ts: %d}) RETURN n`,
		marker, time.Now().UnixMilli(),
	)
	writeStart := time.Now()
	resp, err := postJSON(leader+"/api/cypher", map[string]string{"query": query})
	readAndClose(resp)
	if err != nil || (resp != nil && resp.StatusCode >= 400) {
		return // skip this probe
	}
	_ = writeStart

	// Step 2: Poll each follower until the canary appears (or timeout).
	timeout := 10 * time.Second
	pollInterval := 20 * time.Millisecond

	for _, follower := range followers {
		start := time.Now()
		deadline := start.Add(timeout)
		found := false

		for time.Now().Before(deadline) {
			select {
			case <-ctx.Done():
				return
			default:
			}

			readQuery := fmt.Sprintf(`MATCH (n:_Canary {marker: "%s"}) RETURN n`, marker)
			resp, err := postJSON(follower+"/api/cypher", map[string]string{"query": readQuery})
			if err == nil && resp != nil && resp.StatusCode < 400 {
				var result struct {
					Rows []map[string]any `json:"rows"`
				}
				json.NewDecoder(resp.Body).Decode(&result)
				resp.Body.Close()

				if len(result.Rows) > 0 {
					lag := time.Since(start)
					collector.RecordRepLag(lag)
					found = true
					break
				}
			} else {
				readAndClose(resp)
			}

			time.Sleep(pollInterval)
			// Increase poll interval slightly to avoid hammering.
			if pollInterval < 200*time.Millisecond {
				pollInterval = pollInterval * 3 / 2
			}
		}

		if !found {
			// Record timeout as max lag.
			collector.RecordRepLag(timeout)
		}
	}

	// Cleanup: delete the canary node (best-effort, don't block on errors).
	cleanupQuery := fmt.Sprintf(`MATCH (n:_Canary {marker: "%s"}) DELETE n`, marker)
	resp2, err2 := postJSON(leader+"/api/cypher", map[string]string{"query": cleanupQuery})
	_ = err2
	readAndClose(resp2)
}

// ---------------------------------------------------------------------------
// Health check — verify targets are reachable before starting the benchmark.
// ---------------------------------------------------------------------------

func checkTargets(targets []string) error {
	for _, t := range targets {
		resp, err := http.Get(t + "/api/health")
		if err != nil {
			return fmt.Errorf("target %s unreachable: %w", t, err)
		}
		readAndClose(resp)
		if resp.StatusCode >= 500 {
			return fmt.Errorf("target %s returned HTTP %d", t, resp.StatusCode)
		}
	}
	return nil
}
