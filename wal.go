package graphdb

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// ---------------------------------------------------------------------------
// Write-Ahead Log (WAL)
//
// The WAL is an append-only, segment-rotated log of all committed mutations.
// It serves as the replication stream: followers connect to the leader and
// replay WAL entries to maintain a synchronized copy of the graph.
//
// Architecture:
//
//	┌──────────────────────────────────────────────────────────┐
//	│  Leader DB                                               │
//	│                                                          │
//	│  AddNode() ─► bbolt commit ─► WAL.Append(entry)         │
//	│                                    │                     │
//	│                                    ▼                     │
//	│  ┌──────────┐ ┌──────────┐ ┌──────────┐                 │
//	│  │wal-00001 │ │wal-00002 │ │wal-00003 │  (segments)     │
//	│  └──────────┘ └──────────┘ └──────────┘                 │
//	│                                    │                     │
//	└────────────────────────────────────┼─────────────────────┘
//	                                     │ gRPC StreamWAL
//	                                     ▼
//	                              ┌──────────────┐
//	                              │  Follower DB  │
//	                              │  ApplyLoop()  │
//	                              └──────────────┘
//
// On-disk format per entry:
//
//	┌────────────┬──────────────────────┬────────────┐
//	│ frameLen   │ entry (msgpack)      │ CRC32      │
//	│ (4 bytes)  │ (frameLen bytes)     │ (4 bytes)  │
//	└────────────┴──────────────────────┴────────────┘
//
//	- frameLen: uint32 big-endian, length of the msgpack-encoded entry
//	- entry:    WALEntry serialized with msgpack
//	- CRC32:    Castagnoli checksum over the entry bytes (integrity check)
//
// Segment rotation:
//	When the current segment exceeds walSegmentMaxSize (64MB), a new segment
//	file is created. Segment files are named "wal-NNNNNNNNNN.log" where N
//	is the zero-padded segment number. Old segments are retained until
//	explicitly pruned (after all followers have acknowledged past them).
//
// Thread safety:
//	All WAL methods are safe for concurrent use. The mutex protects file I/O
//	and segment rotation. The nextLSN counter is atomic for lock-free reads.
// ---------------------------------------------------------------------------

const (
	walSegmentMaxSize    = 64 * 1024 * 1024     // 64MB per segment file
	walFilePattern       = "wal-%010d.log"      // segment filename format
	walFrameOverhead     = 4 + 4                // frameLen(4) + crc32(4) = 8 bytes per entry
	walGroupCommitPeriod = 2 * time.Millisecond // how often the background goroutine fsyncs
)

// WAL is the write-ahead log for replication.
//
// Group commit: Instead of fsyncing after every Append(), writes are buffered
// in the OS page cache and a background goroutine fsyncs periodically
// (every walGroupCommitPeriod). This batches multiple writes into a single
// fsync call, dramatically improving throughput under concurrent writes.
//
// Since walAppend is called AFTER bbolt commit (which already fsyncs to its
// own file), losing the last few ms of WAL entries on a crash only affects
// replication — the data is already durable in bbolt. The follower will
// request the missing entries when it reconnects.
type WAL struct {
	mu          sync.Mutex
	dir         string        // directory containing WAL segment files
	currentSeg  *os.File      // open file handle for the active segment
	currentSegN uint64        // current segment number
	currentSize int64         // bytes written to current segment
	nextLSN     atomic.Uint64 // next LSN to assign (atomic for lock-free reads)
	noSync      bool          // skip fsync for testing (unsafe for production)
	log         *slog.Logger
	closed      bool

	// Group commit: background fsync goroutine.
	dirty  atomic.Bool   // true when there are unflushed writes
	stopCh chan struct{} // signals the sync goroutine to stop
	doneCh chan struct{} // closed when the sync goroutine exits
}

// OpenWAL opens or creates a WAL in the given directory.
// It scans existing segments to determine the next LSN (for crash recovery)
// and opens the latest segment for appending.
func OpenWAL(dir string, noSync bool, logger *slog.Logger) (*WAL, error) {
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, fmt.Errorf("graphdb: WAL: failed to create directory: %w", err)
	}

	w := &WAL{
		dir:    walDir,
		noSync: noSync,
		log:    logger,
	}

	// Scan existing segments to find the latest one and recover the next LSN.
	segments, err := w.listSegments()
	if err != nil {
		return nil, fmt.Errorf("graphdb: WAL: failed to list segments: %w", err)
	}

	if len(segments) > 0 {
		// Read the last segment to find the highest LSN.
		lastSeg := segments[len(segments)-1]
		w.currentSegN = lastSeg

		lastLSN, err := w.scanSegmentForLastLSN(lastSeg)
		if err != nil {
			// Segment may be corrupted at the tail (partial write before crash).
			// We log and continue — the next append will write after the last
			// valid entry.
			logger.Warn("WAL: error scanning last segment, may have partial entries",
				"segment", lastSeg, "error", err)
		}
		w.nextLSN.Store(lastLSN + 1)
	} else {
		// Fresh WAL — start at segment 0, LSN 1.
		w.currentSegN = 0
		w.nextLSN.Store(1)
	}

	// Open the current segment for appending.
	segPath := w.segmentPath(w.currentSegN)
	f, err := os.OpenFile(segPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("graphdb: WAL: failed to open segment: %w", err)
	}

	info, _ := f.Stat()
	w.currentSeg = f
	if info != nil {
		w.currentSize = info.Size()
	}

	// Start the group-commit background goroutine (only if fsync is enabled).
	if !w.noSync {
		w.stopCh = make(chan struct{})
		w.doneCh = make(chan struct{})
		go w.syncLoop()
	}

	logger.Info("WAL opened",
		"dir", walDir,
		"segment", w.currentSegN,
		"next_lsn", w.nextLSN.Load(),
		"group_commit", !w.noSync,
	)

	return w, nil
}

// Append writes a new entry to the WAL. It assigns an LSN, serializes the
// entry, writes the frame to disk, and fsyncs (unless noSync is set).
//
// This is called AFTER a successful bbolt commit, so the WAL contains only
// committed mutations. If this method fails, the mutation IS committed in
// bbolt but NOT logged — the operator should be alerted.
//
// Returns the assigned LSN.
func (w *WAL) Append(op OpType, payload []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return 0, fmt.Errorf("graphdb: WAL is closed")
	}

	// Assign LSN.
	lsn := w.nextLSN.Add(1) - 1

	entry := WALEntry{
		LSN:       lsn,
		Timestamp: time.Now().UnixNano(),
		Op:        op,
		Payload:   payload,
	}

	// Serialize the entry.
	entryData, err := msgpack.Marshal(&entry)
	if err != nil {
		return 0, fmt.Errorf("graphdb: WAL: failed to marshal entry: %w", err)
	}

	// Build the frame: frameLen(4) + entryData + crc32(4).
	frameLen := uint32(len(entryData))
	checksum := crc32.Checksum(entryData, crc32Table)

	frame := make([]byte, 4+len(entryData)+4)
	binary.BigEndian.PutUint32(frame[0:4], frameLen)
	copy(frame[4:4+len(entryData)], entryData)
	binary.BigEndian.PutUint32(frame[4+len(entryData):], checksum)

	// Write the frame to the OS page cache (no fsync here).
	// The background syncLoop will fsync periodically (group commit).
	if _, err := w.currentSeg.Write(frame); err != nil {
		return 0, fmt.Errorf("graphdb: WAL: failed to write frame: %w", err)
	}
	w.dirty.Store(true) // signal the sync goroutine

	w.currentSize += int64(len(frame))

	// Rotate segment if it exceeds the size limit.
	if w.currentSize >= walSegmentMaxSize {
		if err := w.rotateSegment(); err != nil {
			w.log.Error("WAL: segment rotation failed", "error", err)
			// Non-fatal: we can continue writing to the current segment.
		}
	}

	return lsn, nil
}

// LastLSN returns the last assigned LSN (0 if no entries have been written).
func (w *WAL) LastLSN() uint64 {
	v := w.nextLSN.Load()
	if v == 0 {
		return 0
	}
	return v - 1
}

// Close flushes and closes the WAL.
func (w *WAL) Close() error {
	// Stop the background sync goroutine first.
	if w.stopCh != nil {
		close(w.stopCh)
		<-w.doneCh // wait for it to finish
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true

	if w.currentSeg != nil {
		// Final fsync to flush any remaining dirty data.
		if err := w.currentSeg.Sync(); err != nil {
			return err
		}
		return w.currentSeg.Close()
	}
	return nil
}

// syncLoop is the group commit background goroutine. It periodically fsyncs
// the WAL file to batch multiple writes into a single fsync call.
//
// Critical design: the mutex is held ONLY to grab the segment reference and
// clear the dirty flag — the actual fsync runs WITHOUT the lock. This allows
// Append() calls to proceed concurrently with the fsync, which is essential
// for high write throughput in cluster mode. Without this, the ~25ms fsync
// on macOS blocks all 16 writers, degrading throughput from 385 to 101 ops/s.
//
// Safety: if segment rotation happens during fsync, the rotation code already
// syncs the old segment before closing it, so our fsync on the (now-closed)
// file harmlessly returns an error that we ignore.
func (w *WAL) syncLoop() {
	defer close(w.doneCh)
	ticker := time.NewTicker(walGroupCommitPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopCh:
			// Final sync before exit — hold lock for safety during shutdown.
			w.mu.Lock()
			if w.dirty.Load() && w.currentSeg != nil && !w.closed {
				_ = w.currentSeg.Sync()
				w.dirty.Store(false)
			}
			w.mu.Unlock()
			return
		case <-ticker.C:
			if !w.dirty.CompareAndSwap(true, false) {
				continue // nothing to sync
			}
			// Grab segment ref under lock, then sync OUTSIDE the lock.
			// This is the key optimization: Append() can proceed while
			// we're waiting for the kernel to flush pages to disk.
			w.mu.Lock()
			seg := w.currentSeg
			closed := w.closed
			w.mu.Unlock()

			if seg != nil && !closed {
				if err := seg.Sync(); err != nil {
					// Benign: segment may have been rotated+closed between
					// the unlock and the Sync(). The rotation already synced.
					w.log.Debug("WAL: group commit fsync (may be post-rotation)", "error", err)
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// WAL Reader — used by followers to replay entries from a given LSN.
// ---------------------------------------------------------------------------

// WALReader reads entries sequentially from the WAL starting at a given LSN.
// It transparently crosses segment boundaries.
type WALReader struct {
	wal      *WAL
	segments []uint64 // sorted segment numbers
	segIdx   int      // current segment index
	file     *os.File // current segment file handle
	fromLSN  uint64   // minimum LSN to return
}

// NewReader creates a WALReader that returns entries with LSN >= fromLSN.
// The reader scans segments to find the starting position.
func (w *WAL) NewReader(fromLSN uint64) (*WALReader, error) {
	segments, err := w.listSegments()
	if err != nil {
		return nil, fmt.Errorf("graphdb: WAL reader: failed to list segments: %w", err)
	}

	return &WALReader{
		wal:      w,
		segments: segments,
		fromLSN:  fromLSN,
	}, nil
}

// Next reads the next WAL entry. Returns io.EOF when no more entries are
// currently available. The reader supports tailing: after receiving io.EOF,
// callers can call Next() again later — new entries appended to the active
// segment (or to new segments after rotation) will be returned.
func (r *WALReader) Next() (*WALEntry, error) {
	for {
		// Open the next segment file if needed.
		if r.file == nil {
			if r.segIdx >= len(r.segments) {
				// Refresh segment list — the writer may have rotated.
				segs, err := r.wal.listSegments()
				if err != nil {
					return nil, err
				}
				r.segments = segs
				if r.segIdx >= len(r.segments) {
					return nil, io.EOF
				}
			}
			path := r.wal.segmentPath(r.segments[r.segIdx])
			f, err := os.Open(path)
			if err != nil {
				return nil, fmt.Errorf("graphdb: WAL reader: failed to open segment: %w", err)
			}
			r.file = f
		}

		// Read frame header (4 bytes).
		var frameLenBuf [4]byte
		if _, err := io.ReadFull(r.file, frameLenBuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// Check if there are newer segments available.
				segs, _ := r.wal.listSegments()
				if len(segs) > len(r.segments) {
					// New segment exists — this one is complete.
					r.file.Close()
					r.file = nil
					r.segments = segs
					r.segIdx++
					continue
				}
				// No newer segment — we're at the tail of the active segment.
				// Return EOF but keep file open for tailing.
				return nil, io.EOF
			}
			return nil, err
		}
		frameLen := binary.BigEndian.Uint32(frameLenBuf[:])

		// Read entry data + CRC32.
		buf := make([]byte, frameLen+4)
		if _, err := io.ReadFull(r.file, buf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// Partial entry at end of segment (crash during write).
				// Check for newer segments.
				segs, _ := r.wal.listSegments()
				if len(segs) > len(r.segments) {
					r.file.Close()
					r.file = nil
					r.segments = segs
					r.segIdx++
					continue
				}
				return nil, io.EOF
			}
			return nil, err
		}

		entryData := buf[:frameLen]
		storedCRC := binary.BigEndian.Uint32(buf[frameLen:])
		actualCRC := crc32.Checksum(entryData, crc32Table)

		if storedCRC != actualCRC {
			// Corrupted entry — skip to next segment.
			r.wal.log.Warn("WAL reader: CRC mismatch, skipping rest of segment",
				"segment", r.segments[r.segIdx])
			r.file.Close()
			r.file = nil
			r.segIdx++
			continue
		}

		// Deserialize the entry.
		var entry WALEntry
		if err := msgpack.Unmarshal(entryData, &entry); err != nil {
			r.wal.log.Warn("WAL reader: failed to unmarshal entry, skipping",
				"error", err)
			continue
		}

		// Skip entries below the requested LSN.
		if entry.LSN < r.fromLSN {
			continue
		}

		return &entry, nil
	}
}

// Close closes the reader's file handle.
func (r *WALReader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

// ---------------------------------------------------------------------------
// Segment management
// ---------------------------------------------------------------------------

// segmentPath returns the full path for a segment number.
func (w *WAL) segmentPath(segN uint64) string {
	return filepath.Join(w.dir, fmt.Sprintf(walFilePattern, segN))
}

// listSegments returns all segment numbers in sorted order.
func (w *WAL) listSegments() ([]uint64, error) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var segments []uint64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		var segN uint64
		if _, parseErr := fmt.Sscanf(e.Name(), walFilePattern, &segN); parseErr == nil {
			segments = append(segments, segN)
		}
	}
	sort.Slice(segments, func(i, j int) bool { return segments[i] < segments[j] })
	return segments, nil
}

// rotateSegment closes the current segment and opens a new one.
// Must be called with w.mu held.
func (w *WAL) rotateSegment() error {
	if w.currentSeg != nil {
		if err := w.currentSeg.Sync(); err != nil {
			return err
		}
		if err := w.currentSeg.Close(); err != nil {
			return err
		}
	}

	w.currentSegN++
	w.currentSize = 0

	path := w.segmentPath(w.currentSegN)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("graphdb: WAL: failed to create new segment: %w", err)
	}

	w.currentSeg = f
	w.log.Info("WAL segment rotated", "segment", w.currentSegN)
	return nil
}

// scanSegmentForLastLSN reads a segment file and returns the highest LSN found.
// Used during startup to recover the next LSN after a crash.
func (w *WAL) scanSegmentForLastLSN(segN uint64) (uint64, error) {
	path := w.segmentPath(segN)
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var lastLSN uint64
	for {
		// Read frame header.
		var frameLenBuf [4]byte
		if _, err := io.ReadFull(f, frameLenBuf[:]); err != nil {
			break // EOF or partial read
		}
		frameLen := binary.BigEndian.Uint32(frameLenBuf[:])

		// Read entry data + CRC.
		buf := make([]byte, frameLen+4)
		if _, err := io.ReadFull(f, buf); err != nil {
			break // partial entry
		}

		entryData := buf[:frameLen]
		storedCRC := binary.BigEndian.Uint32(buf[frameLen:])
		actualCRC := crc32.Checksum(entryData, crc32Table)
		if storedCRC != actualCRC {
			break // corrupted entry — stop scanning
		}

		var entry WALEntry
		if err := msgpack.Unmarshal(entryData, &entry); err != nil {
			break
		}
		if entry.LSN > lastLSN {
			lastLSN = entry.LSN
		}
	}

	return lastLSN, nil
}

// PruneSegmentsBefore removes segment files that contain only entries with
// LSN < minLSN. Used after all followers have acknowledged past a given LSN
// to reclaim disk space.
func (w *WAL) PruneSegmentsBefore(minLSN uint64) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	segments, err := w.listSegments()
	if err != nil {
		return 0, err
	}

	pruned := 0
	for _, segN := range segments {
		// Never prune the active segment.
		if segN >= w.currentSegN {
			break
		}

		// Check if this segment's highest LSN is below minLSN.
		lastLSN, err := w.scanSegmentForLastLSN(segN)
		if err != nil || lastLSN == 0 {
			continue // skip unreadable segments
		}

		if lastLSN < minLSN {
			path := w.segmentPath(segN)
			if err := os.Remove(path); err != nil {
				w.log.Warn("WAL: failed to prune segment", "segment", segN, "error", err)
				continue
			}
			w.log.Info("WAL: pruned segment", "segment", segN, "last_lsn", lastLSN)
			pruned++
		}
	}

	return pruned, nil
}
