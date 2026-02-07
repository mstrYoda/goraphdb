package graphdb

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// IntegrityError describes a single data corruption issue found during verification.
type IntegrityError struct {
	Shard   int    // shard index
	Bucket  string // bucket name ("nodes" or "edges")
	Key     string // hex-encoded key
	Message string // human-readable description
}

func (e IntegrityError) Error() string {
	return fmt.Sprintf("shard %d, %s[%s]: %s", e.Shard, e.Bucket, e.Key, e.Message)
}

// IntegrityReport is the result of a VerifyIntegrity scan.
type IntegrityReport struct {
	NodesChecked int
	EdgesChecked int
	Errors       []IntegrityError
}

// OK returns true if no integrity errors were found.
func (r *IntegrityReport) OK() bool {
	return len(r.Errors) == 0
}

// VerifyIntegrity scans all node and edge data across all shards, verifying
// CRC32 checksums and structural validity. Returns a report of any corruption found.
// This is a read-only operation safe for concurrent use.
func (db *DB) VerifyIntegrity() (*IntegrityReport, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}

	report := &IntegrityReport{}

	for idx, s := range db.shards {
		err := s.db.View(func(tx *bolt.Tx) error {
			// Verify nodes.
			nodesBucket := tx.Bucket(bucketNodes)
			if nodesBucket != nil {
				err := nodesBucket.ForEach(func(k, v []byte) error {
					report.NodesChecked++
					_, err := decodeProps(v)
					if err != nil {
						report.Errors = append(report.Errors, IntegrityError{
							Shard:   idx,
							Bucket:  "nodes",
							Key:     fmt.Sprintf("%x", k),
							Message: err.Error(),
						})
					}
					return nil // continue scanning even on error
				})
				if err != nil {
					return err
				}
			}

			// Verify edges.
			edgesBucket := tx.Bucket(bucketEdges)
			if edgesBucket != nil {
				err := edgesBucket.ForEach(func(k, v []byte) error {
					report.EdgesChecked++
					_, err := decodeEdge(v)
					if err != nil {
						report.Errors = append(report.Errors, IntegrityError{
							Shard:   idx,
							Bucket:  "edges",
							Key:     fmt.Sprintf("%x", k),
							Message: err.Error(),
						})
					}
					return nil
				})
				if err != nil {
					return err
				}
			}

			return nil
		})
		if err != nil {
			return report, fmt.Errorf("graphdb: integrity check failed on shard %d: %w", idx, err)
		}
	}

	if report.OK() {
		db.log.Info("integrity check passed",
			"nodes_checked", report.NodesChecked,
			"edges_checked", report.EdgesChecked,
		)
	} else {
		db.log.Error("integrity check found errors",
			"nodes_checked", report.NodesChecked,
			"edges_checked", report.EdgesChecked,
			"errors", len(report.Errors),
		)
	}

	return report, nil
}
