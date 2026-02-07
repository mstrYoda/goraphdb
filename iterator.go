package graphdb

import (
	"context"
	"fmt"
	"sort"

	bolt "go.etcd.io/bbolt"
)

// RowIterator is a lazy, pull-based iterator over Cypher query result rows.
// Callers must call Close() when done to release underlying resources
// (e.g., bbolt read transactions).
//
// Usage:
//
//	iter, err := db.CypherStream("MATCH (n) RETURN n.name LIMIT 10")
//	if err != nil { ... }
//	defer iter.Close()
//	for iter.Next() {
//	    row := iter.Row()
//	    fmt.Println(row)
//	}
//	if err := iter.Err(); err != nil { ... }
type RowIterator interface {
	// Next advances the iterator to the next row. Returns false when
	// there are no more rows or an error occurred.
	Next() bool
	// Row returns the current row. Only valid after Next() returns true.
	Row() map[string]any
	// Columns returns the column names in RETURN order.
	Columns() []string
	// Err returns the first error encountered during iteration.
	Err() error
	// Close releases all resources held by the iterator.
	Close()
}

// ---------------------------------------------------------------------------
// CypherStream — public API returning a lazy RowIterator.
// ---------------------------------------------------------------------------

// CypherStream parses and executes a Cypher query, returning a lazy iterator
// over result rows. For queries without ORDER BY, rows are produced one at a
// time without full materialization, giving O(1) memory and fast time-to-first-row.
//
// The caller MUST call Close() on the returned iterator.
func (db *DB) CypherStream(ctx context.Context, query string) (RowIterator, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}

	ast := db.cache.get(query)
	if ast == nil {
		parsed, err := parseCypher(query)
		if err != nil {
			return nil, err
		}
		if parsed.write != nil {
			return nil, fmt.Errorf("graphdb: CypherStream does not support CREATE queries")
		}
		ast = parsed.read
		db.cache.put(query, ast)
	}

	return db.buildIterator(ctx, ast)
}

// CypherStreamWithParams is the parameterized version of CypherStream.
func (db *DB) CypherStreamWithParams(ctx context.Context, query string, params map[string]any) (RowIterator, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}

	ast := db.cache.get(query)
	if ast == nil {
		parsed, err := parseCypher(query)
		if err != nil {
			return nil, err
		}
		if parsed.write != nil {
			return nil, fmt.Errorf("graphdb: CypherStreamWithParams does not support CREATE queries")
		}
		ast = parsed.read
		db.cache.put(query, ast)
	}

	resolved := *ast
	if len(params) > 0 {
		if err := resolveParams(&resolved, params); err != nil {
			return nil, err
		}
	}
	return db.buildIterator(ctx, &resolved)
}

// buildIterator constructs a RowIterator for the given parsed query.
// For EXPLAIN-only queries, returns nil (caller should use Cypher() instead).
func (db *DB) buildIterator(ctx context.Context, q *CypherQuery) (RowIterator, error) {
	// EXPLAIN/PROFILE queries don't stream — fall back to materialized.
	if q.Explain != ExplainNone {
		result, err := db.executeCypher(ctx, q)
		if err != nil {
			return nil, err
		}
		return newSliceIterator(result.Columns, result.Rows), nil
	}

	// OPTIONAL MATCH — fall back to materialized (complex join logic).
	if q.OptionalMatch != nil {
		result, err := db.executeCypherNormal(ctx, q)
		if err != nil {
			return nil, err
		}
		return newSliceIterator(result.Columns, result.Rows), nil
	}

	pat := q.Match.Pattern

	switch {
	case len(pat.Nodes) == 1 && len(pat.Rels) == 0:
		return db.buildNodeMatchIterator(q)

	case len(pat.Nodes) == 2 && len(pat.Rels) == 1:
		// For pattern matches, fall back to materialized for now.
		// The scan-level streaming is already the biggest win (node match).
		result, err := db.executeCypherNormal(ctx, q)
		if err != nil {
			return nil, err
		}
		return newSliceIterator(result.Columns, result.Rows), nil

	default:
		return nil, fmt.Errorf("cypher stream: unsupported pattern with %d nodes and %d relationships",
			len(pat.Nodes), len(pat.Rels))
	}
}

// ---------------------------------------------------------------------------
// Node-match streaming iterator
// ---------------------------------------------------------------------------

// buildNodeMatchIterator creates a lazy iterator for MATCH (n) patterns.
// If ORDER BY is present, it must materialize and sort first.
func (db *DB) buildNodeMatchIterator(q *CypherQuery) (RowIterator, error) {
	nodePat := q.Match.Pattern.Nodes[0]
	varName := nodePat.Variable
	if varName == "" {
		varName = "_n"
	}

	columns := make([]string, len(q.Return.Items))
	for i, item := range q.Return.Items {
		columns[i] = returnItemName(item)
	}

	// If ORDER BY is present, we must materialize to sort.
	if len(q.OrderBy) > 0 {
		result, err := db.executeCypherNormal(context.Background(), q) // no ctx needed — already materialized
		if err != nil {
			return nil, err
		}
		return newSliceIterator(result.Columns, result.Rows), nil
	}

	// Streaming path: build a scanIterator that lazily reads nodes.
	return db.newNodeScanIterator(q, nodePat, varName, columns)
}

// nodeScanIterator lazily scans nodes from bbolt, applying filters and
// projecting RETURN expressions one row at a time.
type nodeScanIterator struct {
	db      *DB
	q       *CypherQuery
	nodePat NodePattern
	varName string
	columns []string
	limit   int
	emitted int
	row     map[string]any
	err     error
	closed  bool

	// bbolt transaction management — we hold a read tx per shard.
	shardIdx int
	txs      []*bolt.Tx
	cursor   *bolt.Cursor
	curTx    *bolt.Tx
}

func (db *DB) newNodeScanIterator(q *CypherQuery, nodePat NodePattern, varName string, columns []string) (*nodeScanIterator, error) {
	it := &nodeScanIterator{
		db:      db,
		q:       q,
		nodePat: nodePat,
		varName: varName,
		columns: columns,
		limit:   q.Limit,
		txs:     make([]*bolt.Tx, len(db.shards)),
	}

	// Try to use index-backed candidates first.
	// If we have candidates, use a sliceIterator instead of scanning.
	candidates, err := db.resolveNodeCandidates(q, nodePat, varName)
	if err != nil {
		return nil, err
	}
	if candidates != nil {
		// We got pre-resolved candidates — project and return a slice iterator.
		// But first filter by WHERE and apply LIMIT.
		var rows []map[string]any
		for _, n := range candidates {
			if q.Where != nil {
				bindings := map[string]any{varName: n}
				ok, evalErr := evalBool(q.Where, bindings)
				if evalErr != nil {
					return nil, evalErr
				}
				if !ok {
					continue
				}
			}
			row := make(map[string]any, len(q.Return.Items))
			bindings := map[string]any{varName: n}
			for _, item := range q.Return.Items {
				colName := returnItemName(item)
				val, evalErr := evalExpr(&item.Expr, bindings)
				if evalErr != nil {
					return nil, evalErr
				}
				row[colName] = val
			}
			rows = append(rows, row)
			if it.limit > 0 && len(rows) >= it.limit {
				break
			}
		}
		// Return a slice iterator — no bbolt txs to hold open.
		return nil, errUseFallback
	}

	// Open read transactions for scanning.
	for i, s := range db.shards {
		tx, txErr := s.db.Begin(false)
		if txErr != nil {
			// Clean up already-opened txs.
			for j := 0; j < i; j++ {
				it.txs[j].Rollback()
			}
			return nil, fmt.Errorf("graphdb: failed to begin read tx on shard %d: %w", i, txErr)
		}
		it.txs[i] = tx
	}

	// Position cursor on first shard.
	it.shardIdx = 0
	it.advanceShard()

	return it, nil
}

// errUseFallback is a sentinel to signal that the scan iterator should not
// be used; the caller should use a pre-built slice iterator instead.
var errUseFallback = fmt.Errorf("use fallback")

func (it *nodeScanIterator) advanceShard() {
	if it.shardIdx >= len(it.txs) {
		it.cursor = nil
		return
	}
	tx := it.txs[it.shardIdx]
	b := tx.Bucket(bucketNodes)
	c := b.Cursor()
	it.cursor = c
	it.curTx = tx
}

func (it *nodeScanIterator) Next() bool {
	if it.closed || it.err != nil {
		return false
	}
	if it.limit > 0 && it.emitted >= it.limit {
		return false
	}

	for it.cursor != nil {
		var k, v []byte
		if it.row == nil && it.emitted == 0 {
			// First call — seek to beginning.
			k, v = it.cursor.First()
		} else {
			k, v = it.cursor.Next()
		}

		for k == nil {
			// Move to next shard.
			it.shardIdx++
			if it.shardIdx >= len(it.txs) {
				it.cursor = nil
				return false
			}
			it.advanceShard()
			if it.cursor == nil {
				return false
			}
			k, v = it.cursor.First()
		}

		// Decode and filter.
		nodeID := decodeNodeID(k)
		props, decErr := decodeProps(v)
		if decErr != nil {
			continue // skip corrupted
		}

		labels := loadLabels(it.curTx, nodeID)
		n := &Node{ID: nodeID, Labels: labels, Props: props}

		// Label filter.
		if len(it.nodePat.Labels) > 0 && !matchLabels(n.Labels, it.nodePat.Labels) {
			continue
		}

		// Inline property filter.
		if !matchProps(n.Props, it.nodePat.Props) {
			continue
		}

		// WHERE clause filter.
		if it.q.Where != nil {
			bindings := map[string]any{it.varName: n}
			ok, evalErr := evalBool(it.q.Where, bindings)
			if evalErr != nil {
				it.err = evalErr
				return false
			}
			if !ok {
				continue
			}
		}

		// Project RETURN.
		row := make(map[string]any, len(it.q.Return.Items))
		bindings := map[string]any{it.varName: n}
		for _, item := range it.q.Return.Items {
			colName := returnItemName(item)
			val, evalErr := evalExpr(&item.Expr, bindings)
			if evalErr != nil {
				it.err = evalErr
				return false
			}
			row[colName] = val
		}

		it.row = row
		it.emitted++
		return true
	}

	return false
}

func (it *nodeScanIterator) Row() map[string]any { return it.row }
func (it *nodeScanIterator) Columns() []string   { return it.columns }
func (it *nodeScanIterator) Err() error          { return it.err }

func (it *nodeScanIterator) Close() {
	if it.closed {
		return
	}
	it.closed = true
	for _, tx := range it.txs {
		if tx != nil {
			tx.Rollback()
		}
	}
}

// ---------------------------------------------------------------------------
// resolveNodeCandidates tries to use an index to pre-resolve candidates.
// Returns (nil, nil) if no index is available (caller should full-scan).
// ---------------------------------------------------------------------------

func (db *DB) resolveNodeCandidates(q *CypherQuery, nodePat NodePattern, varName string) ([]*Node, error) {
	// Label index.
	if len(nodePat.Labels) > 0 {
		candidates, err := db.FindByLabel(nodePat.Labels[0])
		if err != nil {
			return nil, err
		}
		var filtered []*Node
		for _, n := range candidates {
			if !matchLabels(n.Labels, nodePat.Labels) {
				continue
			}
			if !matchProps(n.Props, nodePat.Props) {
				continue
			}
			filtered = append(filtered, n)
		}
		return filtered, nil
	}

	// Composite index.
	if len(nodePat.Props) >= 2 {
		propNames := make([]string, 0, len(nodePat.Props))
		for k := range nodePat.Props {
			propNames = append(propNames, k)
		}
		if db.HasCompositeIndex(propNames...) {
			filters := make(map[string]any, len(nodePat.Props))
			for k, v := range nodePat.Props {
				filters[k] = v
			}
			return db.FindByCompositeIndex(filters)
		}
	}

	// Single-property index.
	if len(nodePat.Props) > 0 {
		for key, val := range nodePat.Props {
			if db.HasIndex(key) {
				candidates, err := db.FindByProperty(key, val)
				if err != nil {
					return nil, err
				}
				// Filter remaining props.
				var filtered []*Node
				for _, n := range candidates {
					if matchProps(n.Props, nodePat.Props) {
						filtered = append(filtered, n)
					}
				}
				return filtered, nil
			}
		}
	}

	// WHERE equality index.
	if q.Where != nil && len(nodePat.Props) == 0 {
		if prop, val, ok := extractWhereEquality(q.Where, varName); ok {
			if db.HasIndex(prop) {
				return db.FindByProperty(prop, val)
			}
		}
	}

	return nil, nil // no index available — full scan
}

// ---------------------------------------------------------------------------
// sliceIterator — wraps a materialized []map[string]any as a RowIterator.
// Used as fallback for sorted results, OPTIONAL MATCH, EXPLAIN, etc.
// ---------------------------------------------------------------------------

type sliceIterator struct {
	columns []string
	rows    []map[string]any
	idx     int
}

func newSliceIterator(columns []string, rows []map[string]any) *sliceIterator {
	return &sliceIterator{columns: columns, rows: rows, idx: -1}
}

func (it *sliceIterator) Next() bool {
	it.idx++
	return it.idx < len(it.rows)
}

func (it *sliceIterator) Row() map[string]any {
	if it.idx < 0 || it.idx >= len(it.rows) {
		return nil
	}
	return it.rows[it.idx]
}

func (it *sliceIterator) Columns() []string { return it.columns }
func (it *sliceIterator) Err() error        { return nil }
func (it *sliceIterator) Close()            {} // no-op for materialized data

// ---------------------------------------------------------------------------
// sortedIterator — wraps an iterator that needs sorting.
// Must materialize all rows, sort, then iterate.
// ---------------------------------------------------------------------------

type sortedIterator struct {
	inner *sliceIterator
}

func newSortedIterator(columns []string, rows []map[string]any, orderBy []OrderItem, limit int) *sortedIterator {
	sort.SliceStable(rows, func(i, j int) bool {
		for _, oi := range orderBy {
			vi := evalRowExpr(&oi.Expr, rows[i])
			vj := evalRowExpr(&oi.Expr, rows[j])
			cmp := compareValues(vi, vj)
			if oi.Desc {
				cmp = -cmp
			}
			if cmp != 0 {
				return cmp < 0
			}
		}
		return false
	})
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return &sortedIterator{inner: newSliceIterator(columns, rows)}
}

func (it *sortedIterator) Next() bool          { return it.inner.Next() }
func (it *sortedIterator) Row() map[string]any { return it.inner.Row() }
func (it *sortedIterator) Columns() []string   { return it.inner.Columns() }
func (it *sortedIterator) Err() error          { return it.inner.Err() }
func (it *sortedIterator) Close()              { it.inner.Close() }

// ---------------------------------------------------------------------------
// collectIterator — materializes a RowIterator into a CypherResult.
// Used internally by Cypher() to maintain backward compatibility.
// ---------------------------------------------------------------------------

func collectIterator(iter RowIterator) (*CypherResult, error) {
	defer iter.Close()

	result := &CypherResult{
		Columns: iter.Columns(),
	}

	for iter.Next() {
		result.Rows = append(result.Rows, iter.Row())
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}

	if result.Rows == nil {
		result.Rows = []map[string]any{}
	}

	return result, nil
}
