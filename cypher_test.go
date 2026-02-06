package graphdb

import (
	"fmt"
	"path/filepath"
	"testing"
)

// setupCypherTestDB creates a test database with a social graph:
//
//	Alice --follows--> Bob --follows--> Charlie --follows--> Diana
//	Alice --likes----> Charlie
//
// Node properties:
//
//	Alice:   {name: "Alice",   age: 30}
//	Bob:     {name: "Bob",     age: 25}
//	Charlie: {name: "Charlie", age: 35}
//	Diana:   {name: "Diana",   age: 28}
func setupCypherTestDB(t *testing.T) (*DB, NodeID, NodeID, NodeID, NodeID) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "cypher_test")
	db, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}

	alice, _ := db.AddNode(Props{"name": "Alice", "age": float64(30)})
	bob, _ := db.AddNode(Props{"name": "Bob", "age": float64(25)})
	charlie, _ := db.AddNode(Props{"name": "Charlie", "age": float64(35)})
	diana, _ := db.AddNode(Props{"name": "Diana", "age": float64(28)})

	db.AddEdge(alice, bob, "FOLLOWS", nil)
	db.AddEdge(bob, charlie, "FOLLOWS", nil)
	db.AddEdge(charlie, diana, "FOLLOWS", nil)
	db.AddEdge(alice, charlie, "LIKES", nil)

	return db, alice, bob, charlie, diana
}

// ---------------------------------------------------------------------------
// Test 1: MATCH (n) RETURN n — all nodes
// ---------------------------------------------------------------------------

func TestCypher_MatchAllNodes(t *testing.T) {
	db, _, _, _, _ := setupCypherTestDB(t)
	defer db.Close()

	result, err := db.Cypher(`MATCH (n) RETURN n`)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Columns) != 1 || result.Columns[0] != "n" {
		t.Fatalf("expected columns [n], got %v", result.Columns)
	}

	if len(result.Rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(result.Rows))
	}

	// Each row should have a *Node under key "n".
	for _, row := range result.Rows {
		n, ok := row["n"].(*Node)
		if !ok {
			t.Fatalf("expected *Node in row, got %T", row["n"])
		}
		if n.Props["name"] == nil {
			t.Fatal("node missing 'name' property")
		}
	}
}

// ---------------------------------------------------------------------------
// Test 2: MATCH (n {name: "Alice"}) RETURN n — property filter
// ---------------------------------------------------------------------------

func TestCypher_MatchByProperty(t *testing.T) {
	db, _, _, _, _ := setupCypherTestDB(t)
	defer db.Close()

	result, err := db.Cypher(`MATCH (n {name: "Alice"}) RETURN n`)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	n := result.Rows[0]["n"].(*Node)
	if n.Props["name"] != "Alice" {
		t.Fatalf("expected Alice, got %v", n.Props["name"])
	}
}

// ---------------------------------------------------------------------------
// Test 3: MATCH (n) WHERE n.age > 25 RETURN n — WHERE clause
// ---------------------------------------------------------------------------

func TestCypher_WhereClause(t *testing.T) {
	db, _, _, _, _ := setupCypherTestDB(t)
	defer db.Close()

	result, err := db.Cypher(`MATCH (n) WHERE n.age > 25 RETURN n`)
	if err != nil {
		t.Fatal(err)
	}

	// Alice (30), Charlie (35), Diana (28) have age > 25. Bob (25) does not.
	if len(result.Rows) != 3 {
		names := make([]string, len(result.Rows))
		for i, r := range result.Rows {
			names[i] = r["n"].(*Node).GetString("name")
		}
		t.Fatalf("expected 3 rows (Alice,Charlie,Diana), got %d: %v", len(result.Rows), names)
	}
}

func TestCypher_WhereClauseAnd(t *testing.T) {
	db, _, _, _, _ := setupCypherTestDB(t)
	defer db.Close()

	result, err := db.Cypher(`MATCH (n) WHERE n.age > 25 AND n.age < 32 RETURN n`)
	if err != nil {
		t.Fatal(err)
	}

	// Alice (30), Diana (28) → both > 25 and < 32.
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
}

// ---------------------------------------------------------------------------
// Test 4: MATCH (a)-[:FOLLOWS]->(b) RETURN a, b — 1-hop pattern
// ---------------------------------------------------------------------------

func TestCypher_SingleHopPattern(t *testing.T) {
	db, _, _, _, _ := setupCypherTestDB(t)
	defer db.Close()

	result, err := db.Cypher(`MATCH (a)-[:FOLLOWS]->(b) RETURN a, b`)
	if err != nil {
		t.Fatal(err)
	}

	// Three FOLLOWS edges: Alice→Bob, Bob→Charlie, Charlie→Diana.
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}

	if len(result.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %v", result.Columns)
	}

	// Verify each row has both a and b as *Node.
	for _, row := range result.Rows {
		a, ok := row["a"].(*Node)
		if !ok {
			t.Fatalf("expected *Node for 'a', got %T", row["a"])
		}
		b, ok := row["b"].(*Node)
		if !ok {
			t.Fatalf("expected *Node for 'b', got %T", row["b"])
		}
		t.Logf("  %s --FOLLOWS--> %s", a.GetString("name"), b.GetString("name"))
	}
}

// ---------------------------------------------------------------------------
// Test 5: MATCH (a {name: "Alice"})-[:FOLLOWS]->(b) RETURN b.name
//         — filtered traversal with property projection
// ---------------------------------------------------------------------------

func TestCypher_FilteredTraversal(t *testing.T) {
	db, _, _, _, _ := setupCypherTestDB(t)
	defer db.Close()

	result, err := db.Cypher(`MATCH (a {name: "Alice"})-[:FOLLOWS]->(b) RETURN b.name`)
	if err != nil {
		t.Fatal(err)
	}

	// Alice follows only Bob.
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	name := result.Rows[0]["b.name"]
	if name != "Bob" {
		t.Fatalf("expected Bob, got %v", name)
	}
}

// ---------------------------------------------------------------------------
// Test 6: MATCH (a)-[:FOLLOWS*1..3]->(b) RETURN b — variable-length path
//
// From Alice, FOLLOWS paths of length 1..3:
//   depth 1: Bob
//   depth 2: Charlie
//   depth 3: Diana
// ---------------------------------------------------------------------------

func TestCypher_VariableLengthPath(t *testing.T) {
	db, _, _, _, _ := setupCypherTestDB(t)
	defer db.Close()

	result, err := db.Cypher(`MATCH (a {name: "Alice"})-[:FOLLOWS*1..3]->(b) RETURN b`)
	if err != nil {
		t.Fatal(err)
	}

	// Alice → Bob (1), Charlie (2), Diana (3) = 3 results.
	// Note: Alice herself at depth 0 is the start node and not included (minHops=1).
	if len(result.Rows) != 3 {
		names := make([]string, len(result.Rows))
		for i, r := range result.Rows {
			names[i] = r["b"].(*Node).GetString("name")
		}
		t.Fatalf("expected 3 rows (Bob,Charlie,Diana), got %d: %v", len(result.Rows), names)
	}
}

func TestCypher_VariableLengthPathMinMax(t *testing.T) {
	db, _, _, _, _ := setupCypherTestDB(t)
	defer db.Close()

	result, err := db.Cypher(`MATCH (a {name: "Alice"})-[:FOLLOWS*2..3]->(b) RETURN b`)
	if err != nil {
		t.Fatal(err)
	}

	// Only depth 2 (Charlie) and depth 3 (Diana).
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
}

// ---------------------------------------------------------------------------
// Test 7: MATCH (a)-[r]->(b) RETURN type(r), b — any edge type
// ---------------------------------------------------------------------------

func TestCypher_AnyEdgeType(t *testing.T) {
	db, alice, _, _, _ := setupCypherTestDB(t)
	defer db.Close()

	// Query all outgoing edges from Alice (FOLLOWS→Bob, LIKES→Charlie).
	result, err := db.Cypher(`MATCH (a {name: "Alice"})-[r]->(b) RETURN type(r), b`)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}

	// Verify we get both FOLLOWS and LIKES.
	labels := map[string]bool{}
	for _, row := range result.Rows {
		label, ok := row["type(r)"].(string)
		if !ok {
			t.Fatalf("expected string for type(r), got %T: %v", row["type(r)"], row["type(r)"])
		}
		labels[label] = true
	}
	if !labels["FOLLOWS"] || !labels["LIKES"] {
		t.Fatalf("expected FOLLOWS and LIKES, got %v", labels)
	}

	_ = alice // silence unused
}

// ---------------------------------------------------------------------------
// Test: ORDER BY + LIMIT
// ---------------------------------------------------------------------------

func TestCypher_OrderByLimit(t *testing.T) {
	db, _, _, _, _ := setupCypherTestDB(t)
	defer db.Close()

	result, err := db.Cypher(`MATCH (n) RETURN n ORDER BY n.age DESC LIMIT 2`)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}

	// Highest ages: Charlie (35), Alice (30).
	first := result.Rows[0]["n"].(*Node)
	second := result.Rows[1]["n"].(*Node)

	if first.GetString("name") != "Charlie" {
		t.Fatalf("expected Charlie first, got %s", first.GetString("name"))
	}
	if second.GetString("name") != "Alice" {
		t.Fatalf("expected Alice second, got %s", second.GetString("name"))
	}
}

// ---------------------------------------------------------------------------
// Test: RETURN with alias
// ---------------------------------------------------------------------------

func TestCypher_ReturnAlias(t *testing.T) {
	db, _, _, _, _ := setupCypherTestDB(t)
	defer db.Close()

	result, err := db.Cypher(`MATCH (n {name: "Alice"}) RETURN n.name AS person_name`)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Columns) != 1 || result.Columns[0] != "person_name" {
		t.Fatalf("expected columns [person_name], got %v", result.Columns)
	}

	if result.Rows[0]["person_name"] != "Alice" {
		t.Fatalf("expected Alice, got %v", result.Rows[0]["person_name"])
	}
}

// ---------------------------------------------------------------------------
// Test: Lexer and parser unit tests
// ---------------------------------------------------------------------------

func TestCypherLexer(t *testing.T) {
	input := `MATCH (n {name: "Alice"}) WHERE n.age > 25 RETURN n`
	tokens, err := tokenize(input)
	if err != nil {
		t.Fatal(err)
	}

	// Verify token count (should end with EOF).
	if tokens[len(tokens)-1].Kind != tokEOF {
		t.Fatal("expected EOF token at end")
	}

	// Spot-check a few tokens.
	if tokens[0].Kind != tokMatch {
		t.Fatalf("expected MATCH, got %s", tokenKindName(tokens[0].Kind))
	}

	// Find the string token "Alice".
	found := false
	for _, tok := range tokens {
		if tok.Kind == tokString && tok.Text == "Alice" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected to find string token 'Alice'")
	}
}

func TestCypherParser_SimpleNode(t *testing.T) {
	q, err := parseCypher(`MATCH (n) RETURN n`)
	if err != nil {
		t.Fatal(err)
	}

	if len(q.Match.Pattern.Nodes) != 1 {
		t.Fatalf("expected 1 node in pattern, got %d", len(q.Match.Pattern.Nodes))
	}
	if q.Match.Pattern.Nodes[0].Variable != "n" {
		t.Fatalf("expected variable 'n', got %q", q.Match.Pattern.Nodes[0].Variable)
	}
}

func TestCypherParser_RelPattern(t *testing.T) {
	q, err := parseCypher(`MATCH (a)-[:FOLLOWS]->(b) RETURN a, b`)
	if err != nil {
		t.Fatal(err)
	}

	if len(q.Match.Pattern.Nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(q.Match.Pattern.Nodes))
	}
	if len(q.Match.Pattern.Rels) != 1 {
		t.Fatalf("expected 1 rel, got %d", len(q.Match.Pattern.Rels))
	}
	rel := q.Match.Pattern.Rels[0]
	if rel.Label != "FOLLOWS" {
		t.Fatalf("expected label FOLLOWS, got %q", rel.Label)
	}
	if rel.Dir != Outgoing {
		t.Fatalf("expected Outgoing direction, got %d", rel.Dir)
	}
}

func TestCypherParser_VarLength(t *testing.T) {
	q, err := parseCypher(`MATCH (a)-[:FOLLOWS*1..3]->(b) RETURN b`)
	if err != nil {
		t.Fatal(err)
	}

	rel := q.Match.Pattern.Rels[0]
	if !rel.VarLength {
		t.Fatal("expected VarLength=true")
	}
	if rel.MinHops != 1 {
		t.Fatalf("expected MinHops=1, got %d", rel.MinHops)
	}
	if rel.MaxHops != 3 {
		t.Fatalf("expected MaxHops=3, got %d", rel.MaxHops)
	}
}

func TestCypherParser_Where(t *testing.T) {
	q, err := parseCypher(`MATCH (n) WHERE n.age > 25 RETURN n`)
	if err != nil {
		t.Fatal(err)
	}

	if q.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	if q.Where.Kind != ExprComparison {
		t.Fatalf("expected comparison, got %d", q.Where.Kind)
	}
	if q.Where.Op != OpGt {
		t.Fatalf("expected '>' operator, got %d", q.Where.Op)
	}
}

// ---------------------------------------------------------------------------
// Test: Error cases
// ---------------------------------------------------------------------------

func TestCypher_ErrorInvalidQuery(t *testing.T) {
	db, _, _, _, _ := setupCypherTestDB(t)
	defer db.Close()

	_, err := db.Cypher(`INVALID QUERY`)
	if err == nil {
		t.Fatal("expected error for invalid query")
	}
}

func TestCypher_ErrorUnterminatedString(t *testing.T) {
	db, _, _, _, _ := setupCypherTestDB(t)
	defer db.Close()

	_, err := db.Cypher(`MATCH (n {name: "Alice}) RETURN n`)
	if err == nil {
		t.Fatal("expected error for unterminated string")
	}
}

// ---------------------------------------------------------------------------
// Test: Concurrent Cypher queries
// ---------------------------------------------------------------------------

func TestCypher_Concurrent(t *testing.T) {
	db, _, _, _, _ := setupCypherTestDB(t)
	defer db.Close()

	errc := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func() {
			result, err := db.Cypher(`MATCH (n) WHERE n.age > 25 RETURN n`)
			if err != nil {
				errc <- err
				return
			}
			if len(result.Rows) != 3 {
				errc <- fmt.Errorf("expected 3 rows, got %d", len(result.Rows))
				return
			}
			errc <- nil
		}()
	}

	for i := 0; i < 10; i++ {
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}
}
