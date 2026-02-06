package graphdb

import (
	"fmt"
	"sync"
)

// ---------------------------------------------------------------------------
// Query Plan Cache — avoids re-parsing identical Cypher query strings.
//
// Every unique query string is lexed + parsed once; the resulting AST is
// stored in a sync.Map for lock-free concurrent reads.  The cache lives
// for the lifetime of the DB instance.
//
// Public API:
//
//	pq, err := db.PrepareCypher("MATCH (n {name: $name}) RETURN n")
//	result, err := db.ExecutePrepared(pq)
//
// db.Cypher() also benefits automatically — it checks the cache first.
// ---------------------------------------------------------------------------

// PreparedQuery is a pre-parsed Cypher query that can be executed
// repeatedly without re-parsing.  Thread-safe and immutable once created.
type PreparedQuery struct {
	raw string       // original query string (for debugging / cache key)
	ast *CypherQuery // parsed AST — never mutated after creation
}

// String returns the original query string.
func (pq *PreparedQuery) String() string { return pq.raw }

// queryCache is a simple unbounded cache keyed by query string.
// sync.Map is ideal here: reads vastly outnumber writes, keys are stable
// strings, and there is no need for eviction (ASTs are tiny).
type queryCache struct {
	m sync.Map // map[string]*CypherQuery
}

// get returns the cached AST for the query string, or nil if not cached.
func (c *queryCache) get(query string) *CypherQuery {
	v, ok := c.m.Load(query)
	if !ok {
		return nil
	}
	return v.(*CypherQuery)
}

// put stores an AST in the cache.
func (c *queryCache) put(query string, ast *CypherQuery) {
	c.m.Store(query, ast)
}

// PrepareCypher parses a Cypher query string and returns a PreparedQuery
// that can be executed many times without re-parsing.
// The result is also cached so that future db.Cypher() calls with the same
// string benefit automatically.
//
// Safe for concurrent use.
func (db *DB) PrepareCypher(query string) (*PreparedQuery, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}

	// Check cache first.
	if ast := db.cache.get(query); ast != nil {
		return &PreparedQuery{raw: query, ast: ast}, nil
	}

	// Parse and cache.
	ast, err := parseCypher(query)
	if err != nil {
		return nil, err
	}

	db.cache.put(query, ast)
	return &PreparedQuery{raw: query, ast: ast}, nil
}

// ExecutePrepared executes a previously prepared Cypher query.
// Safe for concurrent use.
func (db *DB) ExecutePrepared(pq *PreparedQuery) (*CypherResult, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}
	return db.executeCypher(pq.ast)
}
