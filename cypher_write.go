package graphdb

import (
	"context"
	"fmt"
	"time"
)

// ---------------------------------------------------------------------------
// Cypher Write Executor — handles CREATE statements.
//
// Supported:
//   CREATE (n:Label {props})                          — create a single node
//   CREATE (a:L1 {p1})-[:REL {p2}]->(b:L2 {p3})     — create two nodes + edge
//   CREATE (a)-[:REL]->(b), (c:Label {props})         — multiple patterns
//   CREATE ... RETURN n, a, b                         — return created entities
//
// Variable bindings: each named node variable (e.g. "n" in "(n:Person)")
// is bound to the created *Node so that later patterns and RETURN can
// reference it.
// ---------------------------------------------------------------------------

// CypherCreateResult holds the result of a CREATE query execution.
type CypherCreateResult struct {
	Columns []string         // column names if RETURN was specified
	Rows    []map[string]any // projected rows if RETURN was specified
	Stats   CreateStats      // mutation statistics
}

// CreateStats tracks what was created by a CREATE statement.
type CreateStats struct {
	NodesCreated int `json:"nodes_created"`
	EdgesCreated int `json:"edges_created"`
	LabelsSet    int `json:"labels_set"`
	PropsSet     int `json:"props_set"`
}

// executeCreate executes a parsed CypherWrite (CREATE) against the database.
func (db *DB) executeCreate(ctx context.Context, w *CypherWrite) (*CypherCreateResult, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}

	result := &CypherCreateResult{}
	// bindings maps variable names to created *Node values.
	bindings := make(map[string]any)

	for _, cp := range w.Creates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if err := db.executeCreatePattern(ctx, cp, bindings, &result.Stats); err != nil {
			return nil, err
		}
	}

	// Project RETURN if present.
	if w.Return != nil {
		for _, item := range w.Return.Items {
			result.Columns = append(result.Columns, returnItemName(item))
		}

		row := make(map[string]any, len(w.Return.Items))
		for _, item := range w.Return.Items {
			colName := item.Alias
			if colName == "" {
				colName = returnItemName(item)
			}
			val, _ := evalExpr(&item.Expr, bindings)
			row[colName] = val
		}
		result.Rows = append(result.Rows, row)
	}

	return result, nil
}

// executeCreatePattern creates nodes and edges for a single CREATE pattern.
func (db *DB) executeCreatePattern(ctx context.Context, cp CreatePattern, bindings map[string]any, stats *CreateStats) error {
	if len(cp.Nodes) == 0 {
		return fmt.Errorf("cypher exec: CREATE pattern has no nodes")
	}

	// Create (or resolve) each node in the pattern.
	nodeIDs := make([]NodeID, len(cp.Nodes))
	for i, np := range cp.Nodes {
		if err := ctx.Err(); err != nil {
			return err
		}

		// If the variable is already bound (from a previous pattern), reuse it.
		if np.Variable != "" {
			if existing, ok := bindings[np.Variable]; ok {
				if n, ok := existing.(*Node); ok {
					nodeIDs[i] = n.ID
					continue
				}
			}
		}

		// Build props from the node pattern.
		props := make(Props)
		for k, v := range np.Props {
			props[k] = v
			stats.PropsSet++
		}

		id, err := db.AddNode(props)
		if err != nil {
			return fmt.Errorf("cypher exec: CREATE node failed: %w", err)
		}
		nodeIDs[i] = id
		stats.NodesCreated++

		// Set labels if any.
		if len(np.Labels) > 0 {
			if err := db.AddLabel(id, np.Labels...); err != nil {
				return fmt.Errorf("cypher exec: CREATE set labels failed: %w", err)
			}
			stats.LabelsSet += len(np.Labels)
		}

		// Bind the variable.
		if np.Variable != "" {
			node, err := db.getNode(id)
			if err != nil {
				return err
			}
			bindings[np.Variable] = node
		}
	}

	// Create edges between consecutive node pairs.
	for i, rp := range cp.Rels {
		if err := ctx.Err(); err != nil {
			return err
		}

		fromID := nodeIDs[i]
		toID := nodeIDs[i+1]

		// Respect direction: if incoming (<-[]-), swap from/to.
		if rp.Dir == Incoming {
			fromID, toID = toID, fromID
		}

		label := rp.Label
		if label == "" {
			return fmt.Errorf("cypher exec: CREATE relationship requires a label (type)")
		}

		// Edge properties (from rel pattern inline props — currently parsed
		// by parseRelPattern as part of the bracket content, but RelPattern
		// doesn't have a Props field; we can add it later. For now edges are
		// created without properties).
		_, err := db.AddEdge(fromID, toID, label, nil)
		if err != nil {
			return fmt.Errorf("cypher exec: CREATE edge failed: %w", err)
		}
		stats.EdgesCreated++

		// Bind edge variable if named.
		if rp.Variable != "" {
			// Fetch the edge we just created for binding.
			edges, err := db.getEdgesForNode(fromID, Outgoing)
			if err == nil {
				for _, e := range edges {
					if e.To == toID && e.Label == label {
						bindings[rp.Variable] = e
						break
					}
				}
			}
		}
	}

	return nil
}

// CypherCreate executes a CREATE Cypher query string.
// Accepts a context.Context for timeout/cancellation.
// Returns the result with creation statistics and optional RETURN data.
func (db *DB) CypherCreate(ctx context.Context, query string) (*CypherCreateResult, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}

	return safeExecuteResult(func() (*CypherCreateResult, error) {
		parsed, err := parseCypher(query)
		if err != nil {
			return nil, err
		}

		if parsed.write == nil {
			return nil, fmt.Errorf("cypher exec: expected CREATE query, got MATCH")
		}

		start := time.Now()
		result, err := db.executeCreate(ctx, parsed.write)
		elapsed := time.Since(start)

		if db.metrics != nil {
			db.metrics.QueriesTotal.Add(1)
			db.metrics.recordQueryDuration(elapsed)
			if err != nil {
				db.metrics.QueryErrorTotal.Add(1)
			}
		}

		return result, err
	})
}
