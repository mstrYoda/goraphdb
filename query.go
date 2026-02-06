package graphdb

import (
	"context"
	"fmt"
)

// Query represents a graph query that can be built fluently and executed.
type Query struct {
	db         *DB
	startID    *NodeID
	direction  Direction
	edgeLabels []string     // follow only these edge labels (empty = all)
	nodeFilter NodeFilter   // filter nodes in results
	edgeFilter EdgeFilter   // filter edges during traversal
	maxDepth   int          // maximum traversal depth
	limit      int          // max number of results
	offset     int          // skip first N results
	traversal  TraversalType // BFS or DFS
}

// TraversalType specifies the traversal algorithm.
type TraversalType int

const (
	TraversalBFS TraversalType = iota
	TraversalDFS
)

// QueryResult holds the results of a query execution.
type QueryResult struct {
	Nodes []*Node           `json:"nodes"`
	Edges []*Edge           `json:"edges,omitempty"`
	Items []*TraversalResult `json:"items,omitempty"`
	Count int               `json:"count"`
}

// NewQuery creates a new query builder.
func (db *DB) NewQuery() *Query {
	return &Query{
		db:        db,
		direction: Outgoing,
		maxDepth:  -1, // unlimited
		limit:     -1, // unlimited
		traversal: TraversalBFS,
	}
}

// From sets the starting node for the query.
func (q *Query) From(id NodeID) *Query {
	q.startID = &id
	return q
}

// FollowEdge adds an edge label filter — only traverse edges with matching labels.
// Can be called multiple times to allow multiple labels.
func (q *Query) FollowEdge(labels ...string) *Query {
	q.edgeLabels = append(q.edgeLabels, labels...)
	return q
}

// Dir sets the traversal direction.
func (q *Query) Dir(dir Direction) *Query {
	q.direction = dir
	return q
}

// Where adds a node filter to the query.
func (q *Query) Where(filter NodeFilter) *Query {
	q.nodeFilter = filter
	return q
}

// WhereEdge adds an edge filter to the query.
func (q *Query) WhereEdge(filter EdgeFilter) *Query {
	q.edgeFilter = filter
	return q
}

// Depth sets the maximum traversal depth.
func (q *Query) Depth(maxDepth int) *Query {
	q.maxDepth = maxDepth
	return q
}

// Limit sets the maximum number of results.
func (q *Query) Limit(limit int) *Query {
	q.limit = limit
	return q
}

// Offset sets the number of results to skip.
func (q *Query) Offset(offset int) *Query {
	q.offset = offset
	return q
}

// UseBFS sets the traversal algorithm to BFS.
func (q *Query) UseBFS() *Query {
	q.traversal = TraversalBFS
	return q
}

// UseDFS sets the traversal algorithm to DFS.
func (q *Query) UseDFS() *Query {
	q.traversal = TraversalDFS
	return q
}

// Execute runs the query and returns results.
func (q *Query) Execute() (*QueryResult, error) {
	if q.startID == nil {
		return nil, fmt.Errorf("graphdb: query requires a starting node (use .From())")
	}

	// Build the combined edge filter.
	var combinedEdgeFilter EdgeFilter
	if len(q.edgeLabels) > 0 || q.edgeFilter != nil {
		labelSet := make(map[string]bool)
		for _, l := range q.edgeLabels {
			labelSet[l] = true
		}

		combinedEdgeFilter = func(e *Edge) bool {
			if len(labelSet) > 0 && !labelSet[e.Label] {
				return false
			}
			if q.edgeFilter != nil && !q.edgeFilter(e) {
				return false
			}
			return true
		}
	}

	result := &QueryResult{}
	skipped := 0
	collected := 0

	visitor := func(r *TraversalResult) bool {
		// Apply node filter.
		if q.nodeFilter != nil && !q.nodeFilter(r.Node) {
			return true // skip but continue
		}

		// Apply offset.
		if q.offset > 0 && skipped < q.offset {
			skipped++
			return true
		}

		// Apply limit.
		if q.limit > 0 && collected >= q.limit {
			return false // stop
		}

		result.Items = append(result.Items, r)
		result.Nodes = append(result.Nodes, r.Node)
		collected++
		return true
	}

	var err error
	switch q.traversal {
	case TraversalBFS:
		err = q.db.BFS(*q.startID, q.maxDepth, q.direction, combinedEdgeFilter, visitor)
	case TraversalDFS:
		err = q.db.DFS(*q.startID, q.maxDepth, q.direction, combinedEdgeFilter, visitor)
	}

	if err != nil {
		return nil, err
	}

	result.Count = len(result.Nodes)
	return result, nil
}

// ConcurrentQuery holds multiple queries to be executed in parallel.
type ConcurrentQuery struct {
	db      *DB
	queries []*Query
}

// NewConcurrentQuery creates a new concurrent query executor.
func (db *DB) NewConcurrentQuery() *ConcurrentQuery {
	return &ConcurrentQuery{db: db}
}

// Add adds a query to the concurrent execution set.
func (cq *ConcurrentQuery) Add(q *Query) *ConcurrentQuery {
	cq.queries = append(cq.queries, q)
	return cq
}

// Execute runs all queries concurrently and returns results in the same order.
func (cq *ConcurrentQuery) Execute(ctx context.Context) ([]*QueryResult, error) {
	if len(cq.queries) == 0 {
		return nil, nil
	}

	tasks := make([]Task, len(cq.queries))
	for i, q := range cq.queries {
		q := q // capture loop variable
		tasks[i] = func() (interface{}, error) {
			return q.Execute()
		}
	}

	taskResults := cq.db.pool.ExecuteConcurrent(ctx, tasks)

	results := make([]*QueryResult, len(taskResults))
	for i, tr := range taskResults {
		if tr.Err != nil {
			return nil, fmt.Errorf("graphdb: concurrent query %d failed: %w", i, tr.Err)
		}
		if tr.Value != nil {
			results[i] = tr.Value.(*QueryResult)
		}
	}

	return results, nil
}

// ExecuteFunc runs multiple arbitrary functions concurrently using the DB's worker pool.
// This is useful for parallel reads or any concurrent operations on the graph.
func (db *DB) ExecuteFunc(ctx context.Context, fns ...func() (interface{}, error)) ([]interface{}, []error) {
	tasks := make([]Task, len(fns))
	for i, fn := range fns {
		tasks[i] = fn
	}

	taskResults := db.pool.ExecuteConcurrent(ctx, tasks)

	values := make([]interface{}, len(taskResults))
	errors := make([]error, len(taskResults))
	for i, r := range taskResults {
		values[i] = r.Value
		errors[i] = r.Err
	}
	return values, errors
}
