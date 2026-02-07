package graphdb

import (
	"fmt"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// Query Plan — tree of operators returned by EXPLAIN and PROFILE.
// ---------------------------------------------------------------------------

// PlanOperator identifies the type of a query plan node.
type PlanOperator string

const (
	OpAllNodesScan  PlanOperator = "AllNodesScan"
	OpLabelScan     PlanOperator = "NodeByLabelScan"
	OpIndexSeek     PlanOperator = "NodeIndexSeek"
	OpFilter        PlanOperator = "Filter"
	OpExpand        PlanOperator = "Expand(All)"
	OpExpandVarLen  PlanOperator = "VarLengthExpand"
	OpProject       PlanOperator = "Projection"
	OpSort          PlanOperator = "Sort"
	OpLimitOp       PlanOperator = "Limit"
	OpProduceResult PlanOperator = "ProduceResults"
)

// PlanNode is a single operator in the query plan tree.
type PlanNode struct {
	Operator    PlanOperator      // operator type
	Details     string            // human-readable detail (e.g. "n:Person", "n.name = $name")
	EstRows     int               // estimated rows (EXPLAIN) or actual rows (PROFILE)
	ActualRows  int               // actual rows produced (PROFILE only, 0 for EXPLAIN)
	ElapsedTime time.Duration     // wall-clock time in this operator (PROFILE only)
	Args        map[string]string // additional key-value details
	Children    []*PlanNode       // child operators (input sources)
}

// QueryPlan is returned by EXPLAIN and PROFILE queries.
type QueryPlan struct {
	Root    *PlanNode      // the plan tree root
	Profile bool           // true if this is a PROFILE (has actual timing/row data)
	Result  *CypherResult  // non-nil only for PROFILE (the actual query result)
}

// String returns a human-readable multi-line representation of the plan.
func (qp *QueryPlan) String() string {
	var sb strings.Builder
	if qp.Profile {
		sb.WriteString("PROFILE:\n")
	} else {
		sb.WriteString("EXPLAIN:\n")
	}
	qp.Root.format(&sb, "", true)
	return sb.String()
}

func (n *PlanNode) format(sb *strings.Builder, prefix string, isLast bool) {
	connector := "├── "
	if isLast {
		connector = "└── "
	}
	if prefix == "" {
		connector = ""
	}

	sb.WriteString(prefix)
	sb.WriteString(connector)
	sb.WriteString(string(n.Operator))
	if n.Details != "" {
		sb.WriteString(" (")
		sb.WriteString(n.Details)
		sb.WriteString(")")
	}

	if n.ActualRows > 0 || n.ElapsedTime > 0 {
		sb.WriteString(fmt.Sprintf(" [rows=%d, time=%s]", n.ActualRows, n.ElapsedTime.Round(time.Microsecond)))
	} else if n.EstRows > 0 {
		sb.WriteString(fmt.Sprintf(" [est. rows=%d]", n.EstRows))
	}

	sb.WriteString("\n")

	childPrefix := prefix
	if prefix != "" {
		if isLast {
			childPrefix += "    "
		} else {
			childPrefix += "│   "
		}
	}

	for i, child := range n.Children {
		child.format(sb, childPrefix, i == len(n.Children)-1)
	}
}

// ---------------------------------------------------------------------------
// Plan builder — constructs a plan tree from a parsed AST.
// ---------------------------------------------------------------------------

// buildPlan creates a query plan tree for the given AST without executing it.
func buildPlan(q *CypherQuery, db *DB) *PlanNode {
	pat := q.Match.Pattern

	var scanNode *PlanNode

	switch {
	case len(pat.Nodes) == 1 && len(pat.Rels) == 0:
		scanNode = buildNodeMatchPlan(q, db)

	case len(pat.Nodes) == 2 && len(pat.Rels) == 1:
		rel := pat.Rels[0]
		if rel.VarLength {
			scanNode = buildVarLengthPlan(q)
		} else {
			scanNode = buildSingleHopPlan(q, db)
		}

	default:
		scanNode = &PlanNode{Operator: OpAllNodesScan, Details: "unsupported pattern"}
	}

	// Wrap with projection/sort/limit as needed.
	top := scanNode
	if len(q.OrderBy) > 0 {
		top = &PlanNode{Operator: OpSort, Details: formatOrderBy(q.OrderBy), Children: []*PlanNode{top}}
	}
	if q.Limit > 0 {
		top = &PlanNode{Operator: OpLimitOp, Details: fmt.Sprintf("%d", q.Limit), Children: []*PlanNode{top}}
	}

	returnDetails := formatReturn(q.Return)
	top = &PlanNode{Operator: OpProduceResult, Details: returnDetails, Children: []*PlanNode{top}}

	return top
}

func buildNodeMatchPlan(q *CypherQuery, db *DB) *PlanNode {
	np := q.Match.Pattern.Nodes[0]
	varName := np.Variable
	if varName == "" {
		varName = "_n"
	}

	var scan *PlanNode

	// Label-based scan.
	if len(np.Labels) > 0 {
		scan = &PlanNode{
			Operator: OpLabelScan,
			Details:  fmt.Sprintf("%s:%s", varName, strings.Join(np.Labels, ":")),
		}
	} else if len(np.Props) > 0 {
		// Check for index-accelerated lookup.
		for key := range np.Props {
			if db.HasIndex(key) {
				scan = &PlanNode{
					Operator: OpIndexSeek,
					Details:  fmt.Sprintf("%s.%s", varName, key),
				}
				break
			}
		}
		if scan == nil {
			scan = &PlanNode{Operator: OpAllNodesScan, Details: varName}
		}
	} else if q.Where != nil {
		// Check for WHERE-based index.
		if prop, _, ok := extractWhereEquality(q.Where, varName); ok && db.HasIndex(prop) {
			scan = &PlanNode{
				Operator: OpIndexSeek,
				Details:  fmt.Sprintf("%s.%s (from WHERE)", varName, prop),
			}
		} else {
			scan = &PlanNode{Operator: OpAllNodesScan, Details: varName}
		}
	} else {
		scan = &PlanNode{Operator: OpAllNodesScan, Details: varName}
	}

	// Add filter node if there are inline props or WHERE.
	if (len(np.Props) > 0 && scan.Operator != OpIndexSeek) || q.Where != nil {
		filterDetail := ""
		if len(np.Props) > 0 {
			filterDetail = fmt.Sprintf("inline props on %s", varName)
		}
		if q.Where != nil {
			if filterDetail != "" {
				filterDetail += " AND WHERE clause"
			} else {
				filterDetail = "WHERE clause"
			}
		}
		scan = &PlanNode{Operator: OpFilter, Details: filterDetail, Children: []*PlanNode{scan}}
	}

	return scan
}

func buildSingleHopPlan(q *CypherQuery, db *DB) *PlanNode {
	np := q.Match.Pattern.Nodes[0]
	rel := q.Match.Pattern.Rels[0]

	varA := np.Variable
	if varA == "" {
		varA = "_a"
	}

	// Build the source scan.
	source := buildNodeMatchPlan(&CypherQuery{
		Match: MatchClause{Pattern: Pattern{Nodes: []NodePattern{np}}},
	}, db)

	expandDetail := fmt.Sprintf("(%s)-[:%s]->", varA, rel.Label)
	if rel.Dir == Incoming {
		expandDetail = fmt.Sprintf("(%s)<-[:%s]-", varA, rel.Label)
	} else if rel.Dir == Both {
		expandDetail = fmt.Sprintf("(%s)-[:%s]-", varA, rel.Label)
	}

	expand := &PlanNode{
		Operator: OpExpand,
		Details:  expandDetail,
		Children: []*PlanNode{source},
	}

	// Filter on target node / WHERE.
	if q.Where != nil {
		expand = &PlanNode{Operator: OpFilter, Details: "WHERE clause", Children: []*PlanNode{expand}}
	}

	return expand
}

func buildVarLengthPlan(q *CypherQuery) *PlanNode {
	rel := q.Match.Pattern.Rels[0]
	detail := fmt.Sprintf("[:%s*%d..%d]", rel.Label, rel.MinHops, rel.MaxHops)
	return &PlanNode{
		Operator: OpExpandVarLen,
		Details:  detail,
		Children: []*PlanNode{
			{Operator: OpAllNodesScan, Details: "source"},
		},
	}
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

func formatOrderBy(items []OrderItem) string {
	var parts []string
	for _, item := range items {
		s := formatExprBrief(item.Expr)
		if item.Desc {
			s += " DESC"
		}
		parts = append(parts, s)
	}
	return strings.Join(parts, ", ")
}

func formatReturn(rc ReturnClause) string {
	var parts []string
	for _, item := range rc.Items {
		s := formatExprBrief(item.Expr)
		if item.Alias != "" {
			s += " AS " + item.Alias
		}
		parts = append(parts, s)
	}
	return strings.Join(parts, ", ")
}

func formatExprBrief(e Expression) string {
	switch e.Kind {
	case ExprVarRef:
		return e.Variable
	case ExprPropAccess:
		return e.Variable + "." + e.Property
	case ExprFuncCall:
		return e.FuncName + "(...)"
	case ExprLiteral:
		return fmt.Sprintf("%v", e.LitValue)
	case ExprParam:
		return "$" + e.ParamName
	default:
		return "expr"
	}
}
