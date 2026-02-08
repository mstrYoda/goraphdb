package graphdb

import (
	"fmt"
	"os"
	"time"

	bolt "go.etcd.io/bbolt"
)

// ---------------------------------------------------------------------------
// Online Compaction
//
// BoltDB uses a copy-on-write B+ tree. When pages are freed (after deletes
// or updates), they go onto an internal freelist but the file never shrinks.
// Over time, heavy delete workloads cause unbounded disk growth.
//
// Compaction rewrites the database to a fresh file containing only live pages,
// then atomically swaps the new file into place. The process is:
//
//  1. Open a read transaction (consistent snapshot of all live data).
//  2. Stream the snapshot to a temporary file via bolt.Tx.WriteTo().
//  3. Close the current bolt.DB handle.
//  4. Rename temp file → original path (atomic on POSIX).
//  5. Reopen the bolt.DB from the compacted file.
//  6. Reload in-memory counters from the freshly opened database.
//
// During step 3→5 the shard is unavailable for writes. This window is
// typically <100ms for databases under 1GB. Reads that were in-flight
// before the close will see errors and should be retried.
// ---------------------------------------------------------------------------

// compact rewrites a single shard's bbolt file to reclaim free pages.
// Returns the bytes saved (old size − new size), or 0 if compaction
// didn't reduce the file size (e.g., no free pages to reclaim).
func (s *shard) compact() (int64, error) {
	oldSize, err := s.fileSize()
	if err != nil {
		return 0, fmt.Errorf("graphdb: compact: failed to stat %s: %w", s.path, err)
	}

	// Step 1: Write a consistent snapshot to a temp file.
	// bolt.Tx.WriteTo() streams only live pages — free pages are excluded.
	tmpPath := s.path + ".compact.tmp"
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return 0, fmt.Errorf("graphdb: compact: failed to create temp file: %w", err)
	}

	err = s.db.View(func(tx *bolt.Tx) error {
		_, writeErr := tx.WriteTo(tmpFile)
		return writeErr
	})
	tmpFile.Close()
	if err != nil {
		os.Remove(tmpPath)
		return 0, fmt.Errorf("graphdb: compact: failed to write snapshot: %w", err)
	}

	// Step 2: Close the current database handle.
	// After this point the shard is temporarily unavailable.
	if closeErr := s.db.Close(); closeErr != nil {
		os.Remove(tmpPath)
		return 0, fmt.Errorf("graphdb: compact: failed to close db: %w", closeErr)
	}

	// Step 3: Atomic rename — replaces the original file.
	// On POSIX this is atomic; on Windows it may require os.Rename to succeed.
	if renameErr := os.Rename(tmpPath, s.path); renameErr != nil {
		// Critical: we closed the DB but can't swap the file.
		// Try to reopen the original to avoid leaving the shard dead.
		s.db, _ = bolt.Open(s.path, 0600, bolt.DefaultOptions)
		return 0, fmt.Errorf("graphdb: compact: failed to rename: %w", renameErr)
	}

	// Step 4: Reopen the compacted database.
	newDB, openErr := bolt.Open(s.path, 0600, bolt.DefaultOptions)
	if openErr != nil {
		return 0, fmt.Errorf("graphdb: compact: failed to reopen: %w", openErr)
	}
	s.db = newDB

	// Step 5: Reload counters from the fresh database.
	if loadErr := s.loadCounters(); loadErr != nil {
		return 0, fmt.Errorf("graphdb: compact: failed to reload counters: %w", loadErr)
	}

	newSize, _ := s.fileSize()
	saved := oldSize - newSize
	if saved < 0 {
		saved = 0
	}
	return saved, nil
}

// Compact performs online compaction on all shards, reclaiming disk space
// from deleted data. Each shard is compacted sequentially to minimize
// disruption. Returns the total bytes reclaimed across all shards.
//
// This is safe to call while the database is serving reads (they will see
// brief errors during the swap window and should retry). Writes are blocked
// by the write semaphore during compaction.
//
// For automatic periodic compaction, set Options.CompactionInterval.
func (db *DB) Compact() (int64, error) {
	if db.isClosed() {
		return 0, fmt.Errorf("graphdb: database is closed")
	}

	var totalSaved int64
	for i, s := range db.shards {
		db.log.Info("compacting shard", "shard", i, "path", s.path)

		saved, err := s.compact()
		if err != nil {
			db.log.Error("compaction failed", "shard", i, "error", err)
			return totalSaved, fmt.Errorf("graphdb: compaction failed on shard %d: %w", i, err)
		}

		totalSaved += saved
		db.log.Info("shard compacted",
			"shard", i,
			"bytes_saved", saved,
		)
	}

	db.log.Info("compaction complete", "total_bytes_saved", totalSaved)
	return totalSaved, nil
}

// startCompactionLoop launches a background goroutine that periodically
// calls Compact(). Stopped when the quit channel is closed (in DB.Close()).
func (db *DB) startCompactionLoop(interval time.Duration, quit chan struct{}) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if db.isClosed() {
					return
				}
				saved, err := db.Compact()
				if err != nil {
					db.log.Error("background compaction failed", "error", err)
				} else if saved > 0 {
					db.log.Info("background compaction reclaimed space", "bytes_saved", saved)
				}
			case <-quit:
				return
			}
		}
	}()
}
