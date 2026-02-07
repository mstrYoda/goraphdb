package graphdb

import (
	"time"
)

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

	db.log.Warn("slow query detected",
		"query", truncateQuery(query, 200),
		"duration", duration.String(),
		"duration_ms", float64(duration.Microseconds())/1000.0,
		"rows", rowCount,
		"threshold", threshold.String(),
	)
}

// truncateQuery truncates a query string to maxLen for logging.
func truncateQuery(q string, maxLen int) string {
	if len(q) <= maxLen {
		return q
	}
	return q[:maxLen] + "..."
}
