package graphdb

// --------------------------------------------------------------------------
// Cypher AST — Abstract Syntax Tree types for a subset of the Cypher query
// language.  These are produced by the parser and consumed by the executor.
// --------------------------------------------------------------------------

// CypherQuery is the top-level AST node for a read query.
//
//	MATCH <pattern> [WHERE <expr>] RETURN <items> [ORDER BY <items>] [LIMIT <n>]
type CypherQuery struct {
	Match   MatchClause
	Where   *Expression // nil when there is no WHERE
	Return  ReturnClause
	OrderBy []OrderItem // nil when there is no ORDER BY
	Limit   int         // 0 means no limit
}

// MatchClause holds the pattern that follows the MATCH keyword.
type MatchClause struct {
	Pattern Pattern
}

// ---------------------------------------------------------------------------
// Pattern — a chain of alternating nodes and relationships.
//
//   (a)-[:FOLLOWS]->(b)-[:LIKES]->(c)
//
// is represented as:
//   Nodes: [a, b, c]
//   Rels:  [FOLLOWS, LIKES]       (len = len(Nodes)-1)
// ---------------------------------------------------------------------------

// Pattern is a sequence of node–rel–node–rel–…–node.
type Pattern struct {
	Nodes []NodePattern
	Rels  []RelPattern // len(Rels) == len(Nodes)-1
}

// NodePattern represents a single node in a MATCH pattern.
//
//	(n)                → Variable="n", Labels=nil, Props=nil
//	(n:Person)         → Variable="n", Labels=["Person"], Props=nil
//	(n:Person:Actor)   → Variable="n", Labels=["Person","Actor"], Props=nil
//	(n {name:"Alice"}) → Variable="n", Labels=nil, Props={"name":"Alice"}
//	()                 → anonymous node
type NodePattern struct {
	Variable string         // binding variable, may be ""
	Labels   []string       // label constraints, may be nil
	Props    map[string]any // inline property constraints, may be nil
}

// RelPattern represents a relationship (edge) in a MATCH pattern.
//
//	-[:FOLLOWS]->       → Label="FOLLOWS", Dir=Outgoing
//	-[r:FOLLOWS]->      → Variable="r", Label="FOLLOWS", Dir=Outgoing
//	-[r]->              → Variable="r", Label="" (any label)
//	-[:FOLLOWS*1..3]->  → VarLength=true, MinHops=1, MaxHops=3
//	<-[:FOLLOWS]-       → Dir=Incoming
//	-[:FOLLOWS]-        → Dir=Both
type RelPattern struct {
	Variable  string
	Label     string    // empty = match any edge label
	Dir       Direction // Outgoing, Incoming, Both
	VarLength bool      // true when * is present
	MinHops   int       // default 1
	MaxHops   int       // default -1 (unlimited)
}

// ---------------------------------------------------------------------------
// RETURN clause
// ---------------------------------------------------------------------------

// ReturnClause holds the items after RETURN.
type ReturnClause struct {
	Items []ReturnItem
}

// ReturnItem is a single expression in the RETURN clause, optionally aliased.
//
//	RETURN a            → Expr=VarRef("a"), Alias=""
//	RETURN b.name       → Expr=PropAccess("b","name"), Alias=""
//	RETURN type(r)      → Expr=FuncCall("type", VarRef("r")), Alias=""
//	RETURN a AS person  → Expr=VarRef("a"), Alias="person"
type ReturnItem struct {
	Expr  Expression
	Alias string // "" if no AS
}

// OrderItem is a single expression in ORDER BY.
type OrderItem struct {
	Expr Expression
	Desc bool // true for DESC, false for ASC (default)
}

// ---------------------------------------------------------------------------
// Expressions
// ---------------------------------------------------------------------------

// ExprKind distinguishes different expression types.
type ExprKind int

const (
	ExprLiteral    ExprKind = iota // string, int, float, bool, nil
	ExprVarRef                     // n
	ExprPropAccess                 // n.name
	ExprFuncCall                   // type(r)
	ExprComparison                 // n.age > 25
	ExprAnd                        // expr AND expr
	ExprOr                         // expr OR expr
	ExprNot                        // NOT expr
	ExprParam                      // $paramName
)

// CompOp is a comparison operator.
type CompOp int

const (
	OpEq  CompOp = iota // =
	OpNeq               // <>
	OpLt                // <
	OpGt                // >
	OpLte               // <=
	OpGte               // >=
)

// Expression is a polymorphic AST node for all expression types.
// Only the fields relevant to the Kind are populated.
type Expression struct {
	Kind ExprKind

	// ExprLiteral
	LitValue any // string | float64 | int64 | bool | nil

	// ExprVarRef
	Variable string

	// ExprPropAccess
	Object   string // variable name
	Property string // property key

	// ExprFuncCall
	FuncName string
	Args     []Expression

	// ExprComparison
	Left  *Expression
	Op    CompOp
	Right *Expression

	// ExprAnd / ExprOr
	Operands []Expression

	// ExprNot
	Inner *Expression

	// ExprParam
	ParamName string // parameter name without the '$' prefix
}

// paramRef is a sentinel type used in property maps to represent a $param reference.
// When the query is executed with parameters, paramRef values are resolved to actual values.
type paramRef string

// Convenience constructors ------------------------------------------------

func litExpr(v any) Expression {
	return Expression{Kind: ExprLiteral, LitValue: v}
}

func varRefExpr(name string) Expression {
	return Expression{Kind: ExprVarRef, Variable: name}
}

func propExpr(obj, prop string) Expression {
	return Expression{Kind: ExprPropAccess, Object: obj, Property: prop}
}

func funcCallExpr(name string, args ...Expression) Expression {
	return Expression{Kind: ExprFuncCall, FuncName: name, Args: args}
}

func compExpr(left Expression, op CompOp, right Expression) Expression {
	return Expression{Kind: ExprComparison, Left: &left, Op: op, Right: &right}
}

func andExpr(operands ...Expression) Expression {
	return Expression{Kind: ExprAnd, Operands: operands}
}

func orExpr(operands ...Expression) Expression {
	return Expression{Kind: ExprOr, Operands: operands}
}

func notExpr(inner Expression) Expression {
	return Expression{Kind: ExprNot, Inner: &inner}
}
