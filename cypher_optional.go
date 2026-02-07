package graphdb

import (
	"context"
	"strings"
)

// ---------------------------------------------------------------------------
// OPTIONAL MATCH — left-outer-join semantics.
//
// MATCH (n:Person) OPTIONAL MATCH (n)-[r:WROTE]->(b) RETURN n, b
//
// For each row from the main MATCH, the optional pattern is attempted.
// If the optional pattern matches, one row per match is emitted.
// If not, a single row is emitted with nil for unmatched bindings.
// ---------------------------------------------------------------------------

// execWithOptionalMatch handles queries that have an OPTIONAL MATCH clause.
// It executes the main MATCH first, then for each result row attempts the
// optional pattern expansion.
func (db *DB) execWithOptionalMatch(_ context.Context, q *CypherQuery) (*CypherResult, error) {
	// Step 1: Execute the main MATCH to get primary bindings.
	mainQ := &CypherQuery{
		Match: q.Match,
		Where: q.Where,
	}

	mainBindings, err := db.collectMainBindings(mainQ)
	if err != nil {
		return nil, err
	}

	optPat := q.OptionalMatch.Pattern

	// Step 2: For each main binding, attempt the optional match.
	var allBindings []map[string]any

	for _, binding := range mainBindings {
		optRows, err := db.attemptOptionalMatch(binding, optPat, q.OptionalWhere)
		if err != nil {
			return nil, err
		}

		if len(optRows) == 0 {
			// No optional matches — emit the row with nil for optional bindings.
			nulled := db.nullifyOptionalBindings(binding, optPat)
			allBindings = append(allBindings, nulled)
		} else {
			allBindings = append(allBindings, optRows...)
		}
	}

	// Step 3: Project the combined bindings into the result.
	return db.projectBindings(q, allBindings)
}

// collectMainBindings executes the main MATCH and returns intermediate
// binding maps (variable name → value) before RETURN projection.
func (db *DB) collectMainBindings(q *CypherQuery) ([]map[string]any, error) {
	pat := q.Match.Pattern
	var bindings []map[string]any

	switch {
	case len(pat.Nodes) == 1 && len(pat.Rels) == 0:
		np := pat.Nodes[0]
		varName := np.Variable
		if varName == "" {
			varName = "_n"
		}

		candidates, err := db.findCandidates(np)
		if err != nil {
			return nil, err
		}

		for _, n := range candidates {
			if !matchProps(n.Props, np.Props) {
				continue
			}
			if q.Where != nil {
				b := map[string]any{varName: n}
				ok, err := evalBool(q.Where, b)
				if err != nil {
					return nil, err
				}
				if !ok {
					continue
				}
			}
			bindings = append(bindings, map[string]any{varName: n})
		}

	case len(pat.Nodes) == 2 && len(pat.Rels) == 1:
		aPat := pat.Nodes[0]
		rel := pat.Rels[0]
		bPat := pat.Nodes[1]

		aVar := aPat.Variable
		if aVar == "" {
			aVar = "_a"
		}
		rVar := rel.Variable
		bVar := bPat.Variable
		if bVar == "" {
			bVar = "_b"
		}

		aCandidates, err := db.findCandidates(aPat)
		if err != nil {
			return nil, err
		}

		for _, a := range aCandidates {
			dir := rel.Dir
			edges, err := db.getEdgesForNode(a.ID, dir)
			if err != nil {
				continue
			}

			for _, e := range edges {
				if rel.Label != "" && !strings.EqualFold(e.Label, rel.Label) {
					continue
				}

				targetID := e.To
				if dir == Incoming {
					targetID = e.From
				}

				bNode, err := db.getNode(targetID)
				if err != nil {
					continue
				}

				if len(bPat.Labels) > 0 && !matchLabels(bNode.Labels, bPat.Labels) {
					continue
				}
				if !matchProps(bNode.Props, bPat.Props) {
					continue
				}

				row := map[string]any{aVar: a, bVar: bNode}
				if rVar != "" {
					row[rVar] = e
				}

				if q.Where != nil {
					ok, err := evalBool(q.Where, row)
					if err != nil {
						return nil, err
					}
					if !ok {
						continue
					}
				}

				bindings = append(bindings, row)
			}
		}
	}

	return bindings, nil
}

// attemptOptionalMatch tries to match the optional pattern using the already-bound
// variables from the main MATCH. Returns expanded binding rows.
func (db *DB) attemptOptionalMatch(binding map[string]any, optPat Pattern, optWhere *Expression) ([]map[string]any, error) {
	// The most common case: OPTIONAL MATCH (n)-[r:TYPE]->(b)
	// where n is already bound from the main MATCH.
	if len(optPat.Nodes) == 2 && len(optPat.Rels) == 1 {
		aPat := optPat.Nodes[0]
		rel := optPat.Rels[0]
		bPat := optPat.Nodes[1]

		aVar := aPat.Variable
		if aVar == "" {
			aVar = "_a"
		}
		rVar := rel.Variable
		bVar := bPat.Variable
		if bVar == "" {
			bVar = "_b"
		}

		// Look up the source node from existing bindings.
		boundVal, ok := binding[aVar]
		if !ok {
			return nil, nil // source variable not bound
		}
		sourceNode, ok := boundVal.(*Node)
		if !ok {
			return nil, nil
		}

		dir := rel.Dir
		edges, err := db.getEdgesForNode(sourceNode.ID, dir)
		if err != nil {
			return nil, nil // treat errors as no match for optional
		}

		var rows []map[string]any
		for _, e := range edges {
			if rel.Label != "" && !strings.EqualFold(e.Label, rel.Label) {
				continue
			}

			targetID := e.To
			if dir == Incoming {
				targetID = e.From
			}

			bNode, err := db.getNode(targetID)
			if err != nil {
				continue
			}

			if len(bPat.Labels) > 0 && !matchLabels(bNode.Labels, bPat.Labels) {
				continue
			}
			if !matchProps(bNode.Props, bPat.Props) {
				continue
			}

			// Build the combined row.
			row := make(map[string]any, len(binding)+2)
			for k, v := range binding {
				row[k] = v
			}
			row[bVar] = bNode
			if rVar != "" {
				row[rVar] = e
			}

			// Apply OPTIONAL WHERE.
			if optWhere != nil {
				ok, err := evalBool(optWhere, row)
				if err != nil {
					continue
				}
				if !ok {
					continue
				}
			}

			rows = append(rows, row)
		}
		return rows, nil
	}

	// Single-node optional match: OPTIONAL MATCH (b:Label)
	if len(optPat.Nodes) == 1 && len(optPat.Rels) == 0 {
		np := optPat.Nodes[0]
		varName := np.Variable
		if varName == "" {
			varName = "_opt"
		}

		candidates, err := db.findCandidates(np)
		if err != nil {
			return nil, nil
		}

		var rows []map[string]any
		for _, n := range candidates {
			row := make(map[string]any, len(binding)+1)
			for k, v := range binding {
				row[k] = v
			}
			row[varName] = n

			if optWhere != nil {
				ok, err := evalBool(optWhere, row)
				if err != nil {
					continue
				}
				if !ok {
					continue
				}
			}

			rows = append(rows, row)
		}
		return rows, nil
	}

	return nil, nil
}

// nullifyOptionalBindings creates a copy of the binding with nil for any
// variables introduced by the optional pattern.
func (db *DB) nullifyOptionalBindings(binding map[string]any, optPat Pattern) map[string]any {
	row := make(map[string]any, len(binding)+3)
	for k, v := range binding {
		row[k] = v
	}

	// Add nil for variables from the optional pattern.
	for _, np := range optPat.Nodes {
		if np.Variable != "" {
			if _, exists := row[np.Variable]; !exists {
				row[np.Variable] = nil
			}
		}
	}
	for _, rp := range optPat.Rels {
		if rp.Variable != "" {
			if _, exists := row[rp.Variable]; !exists {
				row[rp.Variable] = nil
			}
		}
	}

	return row
}

// projectBindings projects a list of binding maps into a CypherResult
// using the RETURN clause, ORDER BY, and LIMIT.
func (db *DB) projectBindings(q *CypherQuery, bindings []map[string]any) (*CypherResult, error) {
	result := &CypherResult{}

	// Build columns.
	for _, item := range q.Return.Items {
		result.Columns = append(result.Columns, returnItemName(item))
	}

	// Build rows.
	for _, binding := range bindings {
		row := make(map[string]any, len(q.Return.Items))
		for _, item := range q.Return.Items {
			colName := item.Alias
			if colName == "" {
				colName = returnItemName(item)
			}
			val, _ := evalExpr(&item.Expr, binding)
			row[colName] = val
		}
		result.Rows = append(result.Rows, row)
	}

	// ORDER BY.
	if len(q.OrderBy) > 0 {
		sortRows(result.Rows, q.OrderBy)
	}

	// LIMIT.
	if q.Limit > 0 && len(result.Rows) > q.Limit {
		result.Rows = result.Rows[:q.Limit]
	}

	return result, nil
}
