package graphdb

import (
	"sync"
	"time"
)

// SlowQueryEntry records a single slow query event.
type SlowQueryEntry struct {
	Query      string        `json:"query"`
	Duration   time.Duration `json:"-"`
	DurationMs float64       `json:"duration_ms"`
	Rows       int           `json:"rows"`
	Timestamp  time.Time     `json:"timestamp"`
}

// slowQueryLog is a bounded ring buffer of recent slow queries.
type slowQueryLog struct {
	mu      sync.Mutex
	entries []SlowQueryEntry
	pos     int
	cap     int
	total   int
}

func newSlowQueryLog(capacity int) *slowQueryLog {
	if capacity <= 0 {
		capacity = 100
	}
	return &slowQueryLog{
		entries: make([]SlowQueryEntry, 0, capacity),
		cap:     capacity,
	}
}

func (l *slowQueryLog) add(e SlowQueryEntry) {
	l.mu.Lock()
	if len(l.entries) < l.cap {
		l.entries = append(l.entries, e)
	} else {
		l.entries[l.pos] = e
	}
	l.pos = (l.pos + 1) % l.cap
	l.total++
	l.mu.Unlock()
}

// Recent returns up to the last N slow queries in reverse chronological order.
func (l *slowQueryLog) Recent(n int) []SlowQueryEntry {
	l.mu.Lock()
	defer l.mu.Unlock()

	size := len(l.entries)
	if n <= 0 || n > size {
		n = size
	}
	result := make([]SlowQueryEntry, n)
	for i := 0; i < n; i++ {
		idx := (l.pos - 1 - i + size) % size
		result[i] = l.entries[idx]
	}
	return result
}

// slowQueryCheck logs a warning if a query exceeded the slow query threshold.
// Called after every Cypher execution. No-op if threshold is zero or negative.
func (db *DB) slowQueryCheck(query string, duration time.Duration, rowCount int) {
	threshold := db.opts.SlowQueryThreshold
	if threshold <= 0 {
		return
	}
	if duration < threshold {
		return
	}

	// Increment metrics counter.
	if db.metrics != nil {
		db.metrics.SlowQueries.Add(1)
	}

	// Record in ring buffer for UI access.
	if db.slowLog != nil {
		db.slowLog.add(SlowQueryEntry{
			Query:      truncateQuery(query, 500),
			Duration:   duration,
			DurationMs: float64(duration.Microseconds()) / 1000.0,
			Rows:       rowCount,
			Timestamp:  time.Now(),
		})
	}

	db.log.Warn("slow query detected",
		"query", truncateQuery(query, 200),
		"duration", duration.String(),
		"duration_ms", float64(duration.Microseconds())/1000.0,
		"rows", rowCount,
		"threshold", threshold.String(),
	)
}

// SlowQueries returns the most recent slow queries (up to n).
func (db *DB) SlowQueries(n int) []SlowQueryEntry {
	if db.slowLog == nil {
		return nil
	}
	return db.slowLog.Recent(n)
}

// truncateQuery truncates a query string to maxLen for logging.
func truncateQuery(q string, maxLen int) string {
	if len(q) <= maxLen {
		return q
	}
	return q[:maxLen] + "..."
}
