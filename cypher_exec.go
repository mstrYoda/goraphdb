package graphdb

import (
	"bytes"
	"container/heap"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

// errLimitReached is a sentinel used to stop ForEachNode early when LIMIT is satisfied.
var errLimitReached = errors.New("graphdb: limit reached")

// --------------------------------------------------------------------------
// Cypher Executor — runs a parsed CypherQuery against the graph database.
//
// Public API:
//   result, err := db.Cypher("MATCH (a {name: 'Alice'})-[:FOLLOWS]->(b) RETURN b")
//   for _, row := range result.Rows { ... }
// --------------------------------------------------------------------------

// CypherResult holds the result of a Cypher query execution.
type CypherResult struct {
	Columns []string         // column names in RETURN order
	Rows    []map[string]any // each row is a map of column→value
	Plan    *QueryPlan       // non-nil when EXPLAIN or PROFILE was used
}

// Cypher parses and executes a Cypher query string against the database.
// Identical query strings are cached after the first parse (lock-free).
// Safe for concurrent use.
func (db *DB) Cypher(query string) (*CypherResult, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}

	// Fast path: cache hit — skip lexer+parser entirely.
	ast := db.cache.get(query)
	if ast == nil {
		var err error
		ast, err = parseCypher(query)
		if err != nil {
			return nil, err
		}
		db.cache.put(query, ast)
	}

	return db.executeCypher(ast)
}

// CypherWithParams parses and executes a parameterized Cypher query.
// Parameters are referenced in the query as $name and resolved from the params map.
//
// Example:
//
//	db.CypherWithParams(
//	    "MATCH (n {name: $name}) WHERE n.age > $minAge RETURN n",
//	    map[string]any{"name": "Alice", "minAge": 25},
//	)
func (db *DB) CypherWithParams(query string, params map[string]any) (*CypherResult, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}

	ast := db.cache.get(query)
	if ast == nil {
		var err error
		ast, err = parseCypher(query)
		if err != nil {
			return nil, err
		}
		db.cache.put(query, ast)
	}

	// Deep-copy and resolve parameters so the cached AST is not mutated.
	resolved := *ast
	if err := resolveParams(&resolved, params); err != nil {
		return nil, err
	}

	return db.executeCypher(&resolved)
}

// resolveParams substitutes $param references in the AST with actual values.
func resolveParams(q *CypherQuery, params map[string]any) error {
	// Resolve inline property map params in node patterns.
	for i := range q.Match.Pattern.Nodes {
		np := &q.Match.Pattern.Nodes[i]
		for k, v := range np.Props {
			if ref, ok := v.(paramRef); ok {
				val, exists := params[string(ref)]
				if !exists {
					return fmt.Errorf("graphdb: missing parameter $%s", string(ref))
				}
				np.Props[k] = val
			}
		}
	}

	// Resolve params in WHERE clause.
	if q.Where != nil {
		if err := resolveExprParams(q.Where, params); err != nil {
			return err
		}
	}

	// Resolve params in RETURN expressions.
	for i := range q.Return.Items {
		if err := resolveExprParams(&q.Return.Items[i].Expr, params); err != nil {
			return err
		}
	}

	return nil
}

// resolveExprParams recursively resolves $param references in an expression tree.
func resolveExprParams(expr *Expression, params map[string]any) error {
	if expr == nil {
		return nil
	}

	switch expr.Kind {
	case ExprParam:
		val, exists := params[expr.ParamName]
		if !exists {
			return fmt.Errorf("graphdb: missing parameter $%s", expr.ParamName)
		}
		// Replace the param expression with a literal.
		expr.Kind = ExprLiteral
		expr.LitValue = val
		expr.ParamName = ""

	case ExprComparison:
		if err := resolveExprParams(expr.Left, params); err != nil {
			return err
		}
		if err := resolveExprParams(expr.Right, params); err != nil {
			return err
		}

	case ExprAnd, ExprOr:
		for i := range expr.Operands {
			if err := resolveExprParams(&expr.Operands[i], params); err != nil {
				return err
			}
		}

	case ExprNot:
		if err := resolveExprParams(expr.Inner, params); err != nil {
			return err
		}

	case ExprFuncCall:
		for i := range expr.Args {
			if err := resolveExprParams(&expr.Args[i], params); err != nil {
				return err
			}
		}
	}

	return nil
}

// executeCypher runs a parsed AST against the database.
// Handles EXPLAIN (plan only) and PROFILE (plan + execution with stats).
func (db *DB) executeCypher(q *CypherQuery) (*CypherResult, error) {
	// EXPLAIN: return the plan without executing.
	if q.Explain == ExplainOnly {
		plan := buildPlan(q, db)
		return &CypherResult{
			Plan: &QueryPlan{Root: plan, Profile: false},
		}, nil
	}

	// PROFILE: build plan, execute with timing, attach stats.
	if q.Explain == ExplainProfile {
		plan := buildPlan(q, db)
		start := time.Now()

		// Execute normally (strip explain mode so we don't recurse).
		normalQ := *q
		normalQ.Explain = ExplainNone
		result, err := db.executeCypherNormal(&normalQ)
		if err != nil {
			return nil, err
		}

		elapsed := time.Since(start)
		annotateProfileStats(plan, len(result.Rows), elapsed)

		result.Plan = &QueryPlan{Root: plan, Profile: true, Result: result}
		return result, nil
	}

	return db.executeCypherNormal(q)
}

// executeCypherNormal is the actual execution engine (no EXPLAIN/PROFILE handling).
func (db *DB) executeCypherNormal(q *CypherQuery) (*CypherResult, error) {
	// If there is an OPTIONAL MATCH, handle it with the left-outer-join path.
	if q.OptionalMatch != nil {
		return db.execWithOptionalMatch(q)
	}

	pat := q.Match.Pattern

	// Determine the execution strategy based on pattern shape.
	switch {
	case len(pat.Nodes) == 1 && len(pat.Rels) == 0:
		// Simple node match: MATCH (n) or MATCH (n {prop: val})
		return db.execNodeMatch(q)

	case len(pat.Nodes) == 2 && len(pat.Rels) == 1:
		// Single-hop pattern: MATCH (a)-[r:LABEL]->(b)
		rel := pat.Rels[0]
		if rel.VarLength {
			return db.execVarLengthMatch(q)
		}
		return db.execSingleHopMatch(q)

	default:
		return nil, fmt.Errorf("cypher exec: unsupported pattern with %d nodes and %d relationships",
			len(pat.Nodes), len(pat.Rels))
	}
}

// annotateProfileStats fills in actual row counts and timing on a plan tree.
func annotateProfileStats(node *PlanNode, totalRows int, elapsed time.Duration) {
	// Walk the plan tree and set the actual rows/time on the root.
	// A more sophisticated implementation would instrument each operator separately.
	node.ActualRows = totalRows
	node.ElapsedTime = elapsed
	for _, child := range node.Children {
		child.ActualRows = totalRows
	}
}

// ---------------------------------------------------------------------------
// Strategy 1: Simple node match — MATCH (n) / MATCH (n {name: "Alice"})
// ---------------------------------------------------------------------------

func (db *DB) execNodeMatch(q *CypherQuery) (*CypherResult, error) {
	nodePat := q.Match.Pattern.Nodes[0]
	varName := nodePat.Variable
	if varName == "" {
		varName = "_n" // anonymous
	}

	hasOrderBy := len(q.OrderBy) > 0
	limit := q.Limit

	// ------------------------------------------------------------------
	// Optimization 0: Label-based index lookup.
	//   MATCH (n:Person) → use FindByLabel for fast label-index scan.
	// ------------------------------------------------------------------
	if len(nodePat.Labels) > 0 {
		candidates, err := db.FindByLabel(nodePat.Labels[0])
		if err != nil {
			return nil, err
		}
		var nodes []*Node
		for _, n := range candidates {
			if !matchLabels(n.Labels, nodePat.Labels) {
				continue
			}
			if !matchProps(n.Props, nodePat.Props) {
				continue
			}
			if q.Where != nil {
				bindings := map[string]any{varName: n}
				ok, err := evalBool(q.Where, bindings)
				if err != nil {
					return nil, err
				}
				if !ok {
					continue
				}
			}
			nodes = append(nodes, n)
			if limit > 0 && !hasOrderBy && len(nodes) >= limit {
				break
			}
		}
		return db.projectResults(q, nodes, nil, varName, "")
	}

	// ------------------------------------------------------------------
	// Optimization 1a: Composite index lookup for multi-property patterns.
	//   MATCH (n {city: "Istanbul", age: 30}) → use composite index if available.
	// ------------------------------------------------------------------
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
			candidates, err := db.FindByCompositeIndex(filters)
			if err != nil {
				return nil, err
			}
			var nodes []*Node
			for _, n := range candidates {
				if q.Where != nil {
					bindings := map[string]any{varName: n}
					ok, err := evalBool(q.Where, bindings)
					if err != nil {
						return nil, err
					}
					if !ok {
						continue
					}
				}
				nodes = append(nodes, n)
			}
			return db.projectResults(q, nodes, nil, varName, "")
		}
	}

	// ------------------------------------------------------------------
	// Optimization 1b: Index-accelerated inline property lookup.
	//   MATCH (n {name: "Alice"}) → use FindByProperty if "name" is indexed.
	// ------------------------------------------------------------------
	if len(nodePat.Props) > 0 {
		for key, val := range nodePat.Props {
			if db.HasIndex(key) {
				return db.execNodeMatchWithIndex(q, key, val, nodePat, varName)
			}
		}
	}

	// ------------------------------------------------------------------
	// Optimization 2: Extract equality from WHERE for index lookup.
	//   MATCH (n) WHERE n.city = "Istanbul" → use FindByProperty if "city" is indexed.
	// ------------------------------------------------------------------
	if q.Where != nil && len(nodePat.Props) == 0 {
		if prop, val, ok := extractWhereEquality(q.Where, varName); ok {
			if db.HasIndex(prop) {
				return db.execNodeMatchWhereIndexed(q, prop, val, nodePat, varName)
			}
		}
	}

	// ------------------------------------------------------------------
	// Fallback: full scan with inline filter + LIMIT push-down.
	// ------------------------------------------------------------------
	var nodes []*Node
	err := db.forEachNode(func(n *Node) error {
		// Inline property filter.
		if !matchProps(n.Props, nodePat.Props) {
			return nil
		}
		// WHERE clause filter.
		if q.Where != nil {
			bindings := map[string]any{varName: n}
			ok, err := evalBool(q.Where, bindings)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
		}
		nodes = append(nodes, n)

		// Early exit: if LIMIT with no ORDER BY, stop once we have enough.
		if limit > 0 && !hasOrderBy && len(nodes) >= limit {
			return errLimitReached
		}
		return nil
	})
	if err != nil && !errors.Is(err, errLimitReached) {
		return nil, err
	}

	return db.projectResults(q, nodes, nil, varName, "")
}

// execNodeMatchWithIndex uses a secondary index to resolve inline property filters.
// Example: MATCH (n {name: "Alice"}) — if "name" is indexed, avoids full scan.
func (db *DB) execNodeMatchWithIndex(q *CypherQuery, indexKey string, indexVal any, nodePat NodePattern, varName string) (*CypherResult, error) {
	candidates, err := db.FindByProperty(indexKey, indexVal)
	if err != nil {
		return nil, err
	}

	// Apply remaining inline property filters and WHERE clause.
	var nodes []*Node
	for _, n := range candidates {
		if !matchProps(n.Props, nodePat.Props) {
			continue
		}
		if q.Where != nil {
			bindings := map[string]any{varName: n}
			ok, err := evalBool(q.Where, bindings)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
		}
		nodes = append(nodes, n)
	}

	return db.projectResults(q, nodes, nil, varName, "")
}

// execNodeMatchWhereIndexed uses a secondary index to resolve a simple WHERE equality.
// Example: MATCH (n) WHERE n.city = "Istanbul" — if "city" is indexed, avoids full scan.
func (db *DB) execNodeMatchWhereIndexed(q *CypherQuery, prop string, val any, nodePat NodePattern, varName string) (*CypherResult, error) {
	candidates, err := db.FindByProperty(prop, val)
	if err != nil {
		return nil, err
	}

	// The indexed equality is already satisfied; apply remaining WHERE conditions.
	var nodes []*Node
	for _, n := range candidates {
		if !matchProps(n.Props, nodePat.Props) {
			continue
		}
		if q.Where != nil {
			bindings := map[string]any{varName: n}
			ok, err := evalBool(q.Where, bindings)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
		}
		nodes = append(nodes, n)
	}

	return db.projectResults(q, nodes, nil, varName, "")
}

// extractWhereEquality checks if a WHERE expression is a simple equality comparison
// (e.g. n.city = "Istanbul") that can be resolved via index lookup.
// Returns (propertyName, literalValue, true) if extractable, or ("", nil, false).
func extractWhereEquality(where *Expression, nodeVar string) (string, any, bool) {
	if where == nil {
		return "", nil, false
	}

	// Direct equality: n.prop = literal
	if where.Kind == ExprComparison && where.Op == OpEq {
		if where.Left != nil && where.Left.Kind == ExprPropAccess && where.Left.Object == nodeVar &&
			where.Right != nil && where.Right.Kind == ExprLiteral {
			return where.Left.Property, where.Right.LitValue, true
		}
		// Reversed: literal = n.prop
		if where.Right != nil && where.Right.Kind == ExprPropAccess && where.Right.Object == nodeVar &&
			where.Left != nil && where.Left.Kind == ExprLiteral {
			return where.Right.Property, where.Left.LitValue, true
		}
	}

	// AND expression: extract from first matching operand.
	if where.Kind == ExprAnd {
		for _, op := range where.Operands {
			if prop, val, ok := extractWhereEquality(&op, nodeVar); ok {
				return prop, val, true
			}
		}
	}

	return "", nil, false
}

// ---------------------------------------------------------------------------
// Strategy 2: Single-hop pattern — MATCH (a)-[r:LABEL]->(b)
// ---------------------------------------------------------------------------

func (db *DB) execSingleHopMatch(q *CypherQuery) (*CypherResult, error) {
	pat := q.Match.Pattern
	aPat := pat.Nodes[0]
	rel := pat.Rels[0]
	bPat := pat.Nodes[1]

	aVar := aPat.Variable
	if aVar == "" {
		aVar = "_a"
	}
	bVar := bPat.Variable
	if bVar == "" {
		bVar = "_b"
	}
	rVar := rel.Variable // may be ""

	// ------------------------------------------------------------------
	// Optimization: Edge-type direct scan.
	// When the start node has no inline property filter, scanning the
	// idx_edge_type index directly is faster than loading ALL nodes and
	// then checking each node's adjacency list.  Everything runs inside
	// a single View transaction per shard (great for locality).
	// ------------------------------------------------------------------
	if len(aPat.Props) == 0 && rel.Label != "" && rel.Dir == Outgoing {
		return db.execSingleHopByEdgeType(q, aPat, rel, bPat, aVar, rVar, bVar)
	}

	// Step 1: Find all candidate "a" nodes.
	aCandidates, err := db.findCandidates(aPat)
	if err != nil {
		return nil, err
	}

	// Step 2: For each "a", traverse edges and find matching "b" nodes.
	var rows []resultRow

	for _, a := range aCandidates {
		edges, err := db.getEdgesForNode(a.ID, rel.Dir)
		if err != nil {
			return nil, err
		}

		for _, e := range edges {
			// Filter by edge label if specified.
			if rel.Label != "" && !strings.EqualFold(e.Label, rel.Label) {
				continue
			}

			// Determine the target node.
			targetID := e.To
			if rel.Dir == Incoming {
				targetID = e.From
			}

			bNode, err := db.getNode(targetID)
			if err != nil {
				continue
			}

			// Apply "b" inline property filter.
			if !matchProps(bNode.Props, bPat.Props) {
				continue
			}

			// Apply WHERE clause.
			if q.Where != nil {
				bindings := map[string]any{
					aVar: a,
					bVar: bNode,
				}
				if rVar != "" {
					bindings[rVar] = e
				}
				ok, err := evalBool(q.Where, bindings)
				if err != nil {
					return nil, err
				}
				if !ok {
					continue
				}
			}

			rows = append(rows, resultRow{a: a, r: e, b: bNode})
		}
	}

	// Step 3: Project RETURN items.
	return db.projectPatternResults(q, rows, aVar, rVar, bVar)
}

// execSingleHopByEdgeType uses the idx_edge_type index to directly scan all
// edges of a given type, avoiding the need to load all nodes first.
// Runs in a single View transaction per shard for maximum efficiency.
//
// Applies to: MATCH (a)-[:LABEL]->(b) with no inline filter on "a".
func (db *DB) execSingleHopByEdgeType(q *CypherQuery, aPat NodePattern, rel RelPattern, bPat NodePattern, aVar, rVar, bVar string) (*CypherResult, error) {
	prefix := encodeIndexPrefix(rel.Label)
	var rows []resultRow

	for _, s := range db.shards {
		err := s.db.View(func(tx *bolt.Tx) error {
			idxBucket := tx.Bucket(bucketIdxEdgeTyp)
			edgeBucket := tx.Bucket(bucketEdges)
			nodeBucket := tx.Bucket(bucketNodes)

			c := idxBucket.Cursor()
			for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
				edgeIDBytes := k[len(prefix):]
				if len(edgeIDBytes) < 8 {
					continue
				}
				edgeID := decodeEdgeID(edgeIDBytes)

				// Read edge data.
				edgeData := edgeBucket.Get(encodeEdgeID(edgeID))
				if edgeData == nil {
					continue
				}
				edge, err := decodeEdge(edgeData)
				if err != nil {
					continue
				}

				// Read "a" node (source) — within the same shard tx.
				aData := nodeBucket.Get(encodeNodeID(edge.From))
				if aData == nil {
					continue // source node deleted; skip
				}
				aProps, err := decodeProps(aData)
				if err != nil {
					continue
				}
				aLabels := loadLabels(tx, edge.From)
				aNode := &Node{ID: edge.From, Labels: aLabels, Props: aProps}

				// Apply "a" label filter.
				if len(aPat.Labels) > 0 && !matchLabels(aNode.Labels, aPat.Labels) {
					continue
				}

				// Read "b" node (target).
				// For single-shard mode, target is in the same tx.
				// For multi-shard, the target may be in another shard.
				var bNode *Node
				bData := nodeBucket.Get(encodeNodeID(edge.To))
				if bData != nil {
					bProps, err := decodeProps(bData)
					if err == nil {
						bLabels := loadLabels(tx, edge.To)
						bNode = &Node{ID: edge.To, Labels: bLabels, Props: bProps}
					}
				}

				if bNode == nil {
					// Target node is on a different shard — fetch externally.
					bNode, err = db.getNode(edge.To)
					if err != nil {
						continue
					}
				}

				// Apply "b" label filter.
				if len(bPat.Labels) > 0 && !matchLabels(bNode.Labels, bPat.Labels) {
					continue
				}

				// Apply "b" inline property filter.
				if !matchProps(bNode.Props, bPat.Props) {
					continue
				}

				// Apply WHERE clause.
				if q.Where != nil {
					bindings := map[string]any{
						aVar: aNode,
						bVar: bNode,
					}
					if rVar != "" {
						bindings[rVar] = edge
					}
					ok, err := evalBool(q.Where, bindings)
					if err != nil {
						return err
					}
					if !ok {
						continue
					}
				}

				rows = append(rows, resultRow{a: aNode, r: edge, b: bNode})
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	return db.projectPatternResults(q, rows, aVar, rVar, bVar)
}

// ---------------------------------------------------------------------------
// Strategy 3: Variable-length path — MATCH (a)-[:LABEL*min..max]->(b)
// ---------------------------------------------------------------------------

func (db *DB) execVarLengthMatch(q *CypherQuery) (*CypherResult, error) {
	pat := q.Match.Pattern
	aPat := pat.Nodes[0]
	rel := pat.Rels[0]
	bPat := pat.Nodes[1]

	aVar := aPat.Variable
	if aVar == "" {
		aVar = "_a"
	}
	bVar := bPat.Variable
	if bVar == "" {
		bVar = "_b"
	}

	// Find starting nodes.
	aCandidates, err := db.findCandidates(aPat)
	if err != nil {
		return nil, err
	}

	maxDepth := rel.MaxHops
	if maxDepth < 0 {
		maxDepth = 50 // safety cap
	}
	minDepth := rel.MinHops

	var rows []resultRow

	for _, a := range aCandidates {
		// Use BFS to find all reachable nodes within hop range.
		var edgeFilter EdgeFilter
		if rel.Label != "" {
			label := rel.Label
			edgeFilter = func(e *Edge) bool {
				return strings.EqualFold(e.Label, label)
			}
		}

		err := db.BFS(a.ID, maxDepth, rel.Dir, edgeFilter, func(tr *TraversalResult) bool {
			if tr.Depth < minDepth {
				return true // continue, not deep enough yet
			}

			bNode := tr.Node

			// Apply "b" label filter.
			if len(bPat.Labels) > 0 && !matchLabels(bNode.Labels, bPat.Labels) {
				return true
			}

			// Apply "b" inline property filter.
			if !matchProps(bNode.Props, bPat.Props) {
				return true
			}

			// Apply WHERE clause.
			if q.Where != nil {
				bindings := map[string]any{
					aVar: a,
					bVar: bNode,
				}
				ok, _ := evalBool(q.Where, bindings)
				if !ok {
					return true
				}
			}

			rows = append(rows, resultRow{a: a, b: bNode})
			return true
		})
		if err != nil {
			return nil, err
		}
	}

	return db.projectPatternResults(q, rows, aVar, "", bVar)
}

// ---------------------------------------------------------------------------
// Candidate finding — index-aware
// ---------------------------------------------------------------------------

// findCandidates finds all nodes matching a NodePattern's labels and inline properties.
// Uses label index and secondary property indexes when available to avoid full table scans.
func (db *DB) findCandidates(np NodePattern) ([]*Node, error) {
	// Fast path: label-based lookup using idx_node_label.
	if len(np.Labels) > 0 {
		// Use the first label for index lookup, then filter the rest.
		candidates, err := db.FindByLabel(np.Labels[0])
		if err != nil {
			return nil, err
		}
		// Filter by additional labels and inline props.
		if len(np.Labels) > 1 || len(np.Props) > 0 {
			var filtered []*Node
			for _, n := range candidates {
				if matchLabels(n.Labels, np.Labels) && matchProps(n.Props, np.Props) {
					filtered = append(filtered, n)
				}
			}
			return filtered, nil
		}
		return candidates, nil
	}

	if len(np.Props) == 0 {
		// No inline filter → all nodes.
		var nodes []*Node
		err := db.forEachNode(func(n *Node) error {
			nodes = append(nodes, n)
			return nil
		})
		return nodes, err
	}

	// Try index-accelerated lookup: pick the first indexed property.
	for key, val := range np.Props {
		if db.HasIndex(key) {
			candidates, err := db.FindByProperty(key, val)
			if err != nil {
				return nil, err
			}
			// If there are additional inline props, filter them out.
			if len(np.Props) > 1 {
				var filtered []*Node
				for _, n := range candidates {
					if matchProps(n.Props, np.Props) {
						filtered = append(filtered, n)
					}
				}
				return filtered, nil
			}
			return candidates, nil
		}
	}

	// No index available → full scan fallback.
	return db.FindNodes(func(n *Node) bool {
		return matchProps(n.Props, np.Props)
	})
}

// matchLabels checks if a node's labels contain all required labels.
func matchLabels(nodeLabels, required []string) bool {
	if len(required) == 0 {
		return true
	}
	set := make(map[string]bool, len(nodeLabels))
	for _, l := range nodeLabels {
		set[l] = true
	}
	for _, r := range required {
		if !set[r] {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Result projection
// ---------------------------------------------------------------------------

// projectResults handles RETURN / ORDER BY / LIMIT for simple node matches.
func (db *DB) projectResults(q *CypherQuery, nodes []*Node, edges []*Edge, nodeVar, edgeVar string) (*CypherResult, error) {
	result := &CypherResult{}

	// Build column names.
	for _, item := range q.Return.Items {
		result.Columns = append(result.Columns, returnItemName(item))
	}

	// ------------------------------------------------------------------
	// Optimization: ORDER BY + LIMIT → heap-based top-K selection.
	// Avoids creating N result maps and O(N log N) sort; instead maintains
	// a heap of K items → O(N log K), and only builds K result maps.
	// ------------------------------------------------------------------
	if len(q.OrderBy) > 0 && q.Limit > 0 {
		h := newTopKHeap(q.OrderBy, q.Limit)

		for _, n := range nodes {
			bindings := map[string]any{nodeVar: n}
			sortKey := evalSortKey(q.OrderBy, bindings)
			h.offer(topKItem{sortKey: sortKey, source: n})
		}

		// Extract in correct order and build result rows.
		sorted := h.sorted()
		for _, item := range sorted {
			n := item.source.(*Node)
			bindings := map[string]any{nodeVar: n}
			row := make(map[string]any)
			for _, ri := range q.Return.Items {
				colName := returnItemName(ri)
				val, err := evalExpr(&ri.Expr, bindings)
				if err != nil {
					return nil, err
				}
				row[colName] = val
			}
			result.Rows = append(result.Rows, row)
		}
		return result, nil
	}

	// Build rows (no ORDER BY + LIMIT fast-path).
	for _, n := range nodes {
		bindings := map[string]any{nodeVar: n}
		row := make(map[string]any)
		for _, item := range q.Return.Items {
			colName := returnItemName(item)
			val, err := evalExpr(&item.Expr, bindings)
			if err != nil {
				return nil, err
			}
			row[colName] = val
		}
		result.Rows = append(result.Rows, row)
	}

	// ORDER BY (without LIMIT — full sort)
	if len(q.OrderBy) > 0 {
		sortRows(result.Rows, q.OrderBy)
	}

	// LIMIT (without ORDER BY already handled by LIMIT push-down)
	if q.Limit > 0 && len(result.Rows) > q.Limit {
		result.Rows = result.Rows[:q.Limit]
	}

	return result, nil
}

// projectPatternResults handles RETURN for pattern matches (a)-[r]->(b).
type resultRow struct {
	a *Node
	r *Edge // may be nil (var-length paths)
	b *Node
}

func (db *DB) projectPatternResults(q *CypherQuery, rows []resultRow, aVar, rVar, bVar string) (*CypherResult, error) {
	result := &CypherResult{}

	for _, item := range q.Return.Items {
		result.Columns = append(result.Columns, returnItemName(item))
	}

	// ------------------------------------------------------------------
	// ORDER BY + LIMIT → heap-based top-K.
	// ------------------------------------------------------------------
	if len(q.OrderBy) > 0 && q.Limit > 0 {
		h := newTopKHeap(q.OrderBy, q.Limit)

		for _, rr := range rows {
			bindings := map[string]any{
				aVar: rr.a,
				bVar: rr.b,
			}
			if rVar != "" && rr.r != nil {
				bindings[rVar] = rr.r
			}
			sortKey := evalSortKey(q.OrderBy, bindings)
			h.offer(topKItem{sortKey: sortKey, source: rr})
		}

		sorted := h.sorted()
		for _, item := range sorted {
			rr := item.source.(resultRow)
			bindings := map[string]any{
				aVar: rr.a,
				bVar: rr.b,
			}
			if rVar != "" && rr.r != nil {
				bindings[rVar] = rr.r
			}
			row := make(map[string]any)
			for _, ri := range q.Return.Items {
				colName := returnItemName(ri)
				val, err := evalExpr(&ri.Expr, bindings)
				if err != nil {
					return nil, err
				}
				row[colName] = val
			}
			result.Rows = append(result.Rows, row)
		}
		return result, nil
	}

	for _, rr := range rows {
		bindings := map[string]any{
			aVar: rr.a,
			bVar: rr.b,
		}
		if rVar != "" && rr.r != nil {
			bindings[rVar] = rr.r
		}

		row := make(map[string]any)
		for _, item := range q.Return.Items {
			colName := returnItemName(item)
			val, err := evalExpr(&item.Expr, bindings)
			if err != nil {
				return nil, err
			}
			row[colName] = val
		}
		result.Rows = append(result.Rows, row)
	}

	if len(q.OrderBy) > 0 {
		sortRows(result.Rows, q.OrderBy)
	}

	if q.Limit > 0 && len(result.Rows) > q.Limit {
		result.Rows = result.Rows[:q.Limit]
	}

	return result, nil
}

// ---------------------------------------------------------------------------
// Top-K Heap — selects the K best items from a stream using O(N log K) time.
// ---------------------------------------------------------------------------

// topKItem wraps a sort key and the original source object.
type topKItem struct {
	sortKey []any // pre-evaluated ORDER BY values
	source  any   // *Node or resultRow
}

// topKHeap implements container/heap.Interface.
// The root holds the "worst" item among the current top-K set,
// so it can be cheaply evicted when a better candidate arrives.
type topKHeap struct {
	items   []topKItem
	orderBy []OrderItem
	limit   int
}

func newTopKHeap(orderBy []OrderItem, limit int) *topKHeap {
	return &topKHeap{
		items:   make([]topKItem, 0, limit),
		orderBy: orderBy,
		limit:   limit,
	}
}

func (h *topKHeap) Len() int { return len(h.items) }

// Less: the "worst" item (should appear LAST in the final result) goes to root.
// For DESC: smallest value is worst → min-heap on sort key.
// For ASC: largest value is worst → max-heap on sort key.
func (h *topKHeap) Less(i, j int) bool {
	for idx, oi := range h.orderBy {
		cmp := compareValues(h.items[i].sortKey[idx], h.items[j].sortKey[idx])
		if cmp == 0 {
			continue
		}
		if oi.Desc {
			return cmp < 0 // min-heap: smallest at root (worst for DESC)
		}
		return cmp > 0 // max-heap: largest at root (worst for ASC)
	}
	return false
}

func (h *topKHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

func (h *topKHeap) Push(x any) { h.items = append(h.items, x.(topKItem)) }

func (h *topKHeap) Pop() any {
	n := len(h.items)
	item := h.items[n-1]
	h.items = h.items[:n-1]
	return item
}

// offer adds an item to the heap if it belongs in the top-K.
func (h *topKHeap) offer(item topKItem) {
	if len(h.items) < h.limit {
		heap.Push(h, item)
		return
	}
	// Check if the new item is "better" than the worst (root).
	if h.isBetter(item.sortKey, h.items[0].sortKey) {
		h.items[0] = item
		heap.Fix(h, 0)
	}
}

// isBetter returns true if sortKey a should appear before sortKey b
// in the final result ordering.
func (h *topKHeap) isBetter(a, b []any) bool {
	for idx, oi := range h.orderBy {
		cmp := compareValues(a[idx], b[idx])
		if cmp == 0 {
			continue
		}
		if oi.Desc {
			return cmp > 0 // larger is better for DESC
		}
		return cmp < 0 // smaller is better for ASC
	}
	return false
}

// sorted extracts all items from the heap in correct result order.
func (h *topKHeap) sorted() []topKItem {
	n := len(h.items)
	result := make([]topKItem, n)
	// Pop items from heap: they come out "worst first" → reverse.
	for i := n - 1; i >= 0; i-- {
		result[i] = heap.Pop(h).(topKItem)
	}
	return result
}

// evalSortKey evaluates all ORDER BY expressions for a given bindings context.
func evalSortKey(orderBy []OrderItem, bindings map[string]any) []any {
	key := make([]any, len(orderBy))
	for i, oi := range orderBy {
		val, _ := evalExpr(&oi.Expr, bindings)
		key[i] = val
	}
	return key
}

// returnItemName computes the column name for a return item.
func returnItemName(item ReturnItem) string {
	if item.Alias != "" {
		return item.Alias
	}
	return exprName(item.Expr)
}

// exprName returns a readable name for an expression (used as column name).
func exprName(e Expression) string {
	switch e.Kind {
	case ExprVarRef:
		return e.Variable
	case ExprPropAccess:
		return e.Object + "." + e.Property
	case ExprFuncCall:
		args := make([]string, len(e.Args))
		for i, a := range e.Args {
			args[i] = exprName(a)
		}
		return e.FuncName + "(" + strings.Join(args, ", ") + ")"
	default:
		return "expr"
	}
}

// ---------------------------------------------------------------------------
// Expression evaluation
// ---------------------------------------------------------------------------

// evalExpr evaluates an expression against a set of variable bindings.
func evalExpr(e *Expression, bindings map[string]any) (any, error) {
	switch e.Kind {
	case ExprLiteral:
		return e.LitValue, nil

	case ExprVarRef:
		val, ok := bindings[e.Variable]
		if !ok {
			return nil, fmt.Errorf("cypher exec: unbound variable %q", e.Variable)
		}
		return val, nil

	case ExprPropAccess:
		obj, ok := bindings[e.Object]
		if !ok {
			return nil, fmt.Errorf("cypher exec: unbound variable %q", e.Object)
		}
		if obj == nil {
			return nil, nil // OPTIONAL MATCH: unmatched binding
		}
		return getProperty(obj, e.Property), nil

	case ExprFuncCall:
		return evalFunc(e.FuncName, e.Args, bindings)

	case ExprComparison:
		return evalComparison(e, bindings)

	case ExprAnd:
		for _, op := range e.Operands {
			v, err := evalBool(&op, bindings)
			if err != nil {
				return nil, err
			}
			if !v {
				return false, nil
			}
		}
		return true, nil

	case ExprOr:
		for _, op := range e.Operands {
			v, err := evalBool(&op, bindings)
			if err != nil {
				return nil, err
			}
			if v {
				return true, nil
			}
		}
		return false, nil

	case ExprNot:
		v, err := evalBool(e.Inner, bindings)
		if err != nil {
			return nil, err
		}
		return !v, nil

	default:
		return nil, fmt.Errorf("cypher exec: unsupported expression kind %d", e.Kind)
	}
}

// evalBool evaluates an expression and coerces the result to bool.
func evalBool(e *Expression, bindings map[string]any) (bool, error) {
	val, err := evalExpr(e, bindings)
	if err != nil {
		return false, err
	}
	return toBool(val), nil
}

// evalComparison evaluates a comparison expression.
func evalComparison(e *Expression, bindings map[string]any) (any, error) {
	left, err := evalExpr(e.Left, bindings)
	if err != nil {
		return nil, err
	}
	right, err := evalExpr(e.Right, bindings)
	if err != nil {
		return nil, err
	}

	cmp := compareValues(left, right)

	switch e.Op {
	case OpEq:
		return cmp == 0, nil
	case OpNeq:
		return cmp != 0, nil
	case OpLt:
		return cmp < 0, nil
	case OpGt:
		return cmp > 0, nil
	case OpLte:
		return cmp <= 0, nil
	case OpGte:
		return cmp >= 0, nil
	default:
		return false, nil
	}
}

// evalFunc evaluates a built-in function call.
func evalFunc(name string, args []Expression, bindings map[string]any) (any, error) {
	switch strings.ToLower(name) {
	case "type":
		if len(args) != 1 {
			return nil, fmt.Errorf("cypher exec: type() requires exactly 1 argument")
		}
		val, err := evalExpr(&args[0], bindings)
		if err != nil {
			return nil, err
		}
		if e, ok := val.(*Edge); ok {
			return e.Label, nil
		}
		return nil, nil

	case "id":
		if len(args) != 1 {
			return nil, fmt.Errorf("cypher exec: id() requires exactly 1 argument")
		}
		val, err := evalExpr(&args[0], bindings)
		if err != nil {
			return nil, err
		}
		switch v := val.(type) {
		case *Node:
			return int64(v.ID), nil
		case *Edge:
			return int64(v.ID), nil
		}
		return nil, nil

	default:
		return nil, fmt.Errorf("cypher exec: unknown function %q", name)
	}
}

// ---------------------------------------------------------------------------
// Property access helpers
// ---------------------------------------------------------------------------

// getProperty extracts a property from a Node or Edge.
func getProperty(obj any, prop string) any {
	switch v := obj.(type) {
	case *Node:
		if v.Props != nil {
			return v.Props[prop]
		}
	case *Edge:
		// Built-in edge properties.
		switch prop {
		case "label", "type":
			return v.Label
		default:
			if v.Props != nil {
				return v.Props[prop]
			}
		}
	}
	return nil
}

// matchProps checks whether a node's properties match all constraints.
func matchProps(actual Props, constraints map[string]any) bool {
	if constraints == nil {
		return true
	}
	for key, expected := range constraints {
		actual, ok := actual[key]
		if !ok {
			return false
		}
		if compareValues(actual, expected) != 0 {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Value comparison & coercion
// ---------------------------------------------------------------------------

// toFloat64 attempts to convert a value to float64 for numeric comparisons.
func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	case uint64:
		return float64(n), true
	}
	return 0, false
}

// compareValues compares two arbitrary values. Returns -1, 0, or 1.
func compareValues(a, b any) int {
	// nil handling
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Numeric comparison.
	af, aOk := toFloat64(a)
	bf, bOk := toFloat64(b)
	if aOk && bOk {
		switch {
		case af < bf:
			return -1
		case af > bf:
			return 1
		default:
			return 0
		}
	}

	// String comparison.
	as, aStr := a.(string)
	bs, bStr := b.(string)
	if aStr && bStr {
		return strings.Compare(as, bs)
	}

	// Bool comparison.
	ab, aBool := a.(bool)
	bb, bBool := b.(bool)
	if aBool && bBool {
		if ab == bb {
			return 0
		}
		if !ab {
			return -1
		}
		return 1
	}

	// Fallback: compare string representations.
	return strings.Compare(fmt.Sprint(a), fmt.Sprint(b))
}

// toBool coerces a value to bool.
func toBool(v any) bool {
	if v == nil {
		return false
	}
	switch b := v.(type) {
	case bool:
		return b
	case int64:
		return b != 0
	case float64:
		return b != 0
	case string:
		return b != ""
	}
	return true
}

// ---------------------------------------------------------------------------
// Sorting
// ---------------------------------------------------------------------------

func sortRows(rows []map[string]any, orderItems []OrderItem) {
	sort.SliceStable(rows, func(i, j int) bool {
		for _, oi := range orderItems {
			vi := evalRowExpr(&oi.Expr, rows[i])
			vj := evalRowExpr(&oi.Expr, rows[j])
			cmp := compareValues(vi, vj)
			if cmp == 0 {
				continue
			}
			if oi.Desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
}

// evalRowExpr evaluates an expression directly against a result row.
// For ORDER BY, the row keys are column names — we try to match by name.
func evalRowExpr(e *Expression, row map[string]any) any {
	name := exprName(*e)
	if v, ok := row[name]; ok {
		return v
	}
	// Fallback: if the expression is a prop access like "n.age", look it up.
	if e.Kind == ExprPropAccess {
		if obj, ok := row[e.Object]; ok {
			return getProperty(obj, e.Property)
		}
	}
	return nil
}
