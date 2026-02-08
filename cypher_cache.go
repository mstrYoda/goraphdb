package graphdb

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// ---------------------------------------------------------------------------
// Query Plan Cache — avoids re-parsing identical Cypher query strings.
//
// The cache is a bounded LRU keyed by query string. When the cache is full,
// the least-recently-used entry is evicted. Default capacity: 10,000 entries.
//
// Public API:
//
//	pq, err := db.PrepareCypher("MATCH (n {name: $name}) RETURN n")
//	result, err := db.ExecutePrepared(pq)
//	result, err := db.ExecutePreparedWithParams(pq, map[string]any{"name": "Alice"})
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

// CacheStats holds query cache statistics for observability.
type CacheStats struct {
	Entries  int    `json:"entries"`  // current number of cached ASTs
	Capacity int    `json:"capacity"` // max entries before eviction
	Hits     uint64 `json:"hits"`     // total cache hits
	Misses   uint64 `json:"misses"`   // total cache misses
}

const defaultQueryCacheCapacity = 10_000

// queryCache is a bounded LRU cache keyed by query string.
// Thread-safe via a single mutex (ASTs are tiny, contention is minimal).
type queryCache struct {
	mu       sync.Mutex
	items    map[string]*queryCacheEntry
	head     *queryCacheEntry // most recently used
	tail     *queryCacheEntry // least recently used
	capacity int
	hits     atomic.Uint64
	misses   atomic.Uint64
}

type queryCacheEntry struct {
	key  string
	ast  *CypherQuery
	prev *queryCacheEntry
	next *queryCacheEntry
}

func newQueryCache(capacity int) queryCache {
	if capacity <= 0 {
		capacity = defaultQueryCacheCapacity
	}
	return queryCache{
		items:    make(map[string]*queryCacheEntry, capacity),
		capacity: capacity,
	}
}

// get returns the cached AST for the query string, or nil if not cached.
func (c *queryCache) get(query string) *CypherQuery {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.items[query]
	if !ok {
		c.misses.Add(1)
		return nil
	}
	c.hits.Add(1)
	c.moveToFront(entry)
	return entry.ast
}

// put stores an AST in the cache, evicting the LRU entry if full.
func (c *queryCache) put(query string, ast *CypherQuery) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.items[query]; ok {
		entry.ast = ast
		c.moveToFront(entry)
		return
	}

	entry := &queryCacheEntry{key: query, ast: ast}
	c.items[query] = entry
	c.pushFront(entry)

	if len(c.items) > c.capacity {
		c.evictLRU()
	}
}

// stats returns current cache statistics.
func (c *queryCache) stats() CacheStats {
	c.mu.Lock()
	n := len(c.items)
	cap := c.capacity
	c.mu.Unlock()
	return CacheStats{
		Entries:  n,
		Capacity: cap,
		Hits:     c.hits.Load(),
		Misses:   c.misses.Load(),
	}
}

func (c *queryCache) moveToFront(e *queryCacheEntry) {
	if c.head == e {
		return
	}
	c.removeEntry(e)
	c.pushFront(e)
}

func (c *queryCache) pushFront(e *queryCacheEntry) {
	e.prev = nil
	e.next = c.head
	if c.head != nil {
		c.head.prev = e
	}
	c.head = e
	if c.tail == nil {
		c.tail = e
	}
}

func (c *queryCache) removeEntry(e *queryCacheEntry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		c.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		c.tail = e.prev
	}
	e.prev = nil
	e.next = nil
}

func (c *queryCache) evictLRU() {
	if c.tail == nil {
		return
	}
	victim := c.tail
	c.removeEntry(victim)
	delete(c.items, victim.key)
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
	parsed, err := parseCypher(query)
	if err != nil {
		return nil, err
	}
	if parsed.write != nil {
		return nil, fmt.Errorf("graphdb: PrepareCypher does not support CREATE queries")
	}

	db.cache.put(query, parsed.read)
	return &PreparedQuery{raw: query, ast: parsed.read}, nil
}

// ExecutePrepared executes a previously prepared Cypher query.
// Accepts a context.Context for timeout/cancellation.
// Safe for concurrent use.
func (db *DB) ExecutePrepared(ctx context.Context, pq *PreparedQuery) (*CypherResult, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}
	return safeExecuteResult(func() (*CypherResult, error) {
		ctx, cancel := db.governor.wrapContext(ctx)
		defer cancel()
		return db.executeCypher(ctx, pq.ast)
	})
}

// ExecutePreparedWithParams executes a prepared query with parameter substitution.
// Accepts a context.Context for timeout/cancellation.
// The cached AST is deep-copied before parameter resolution so it remains immutable.
// Safe for concurrent use.
func (db *DB) ExecutePreparedWithParams(ctx context.Context, pq *PreparedQuery, params map[string]any) (*CypherResult, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}
	return safeExecuteResult(func() (*CypherResult, error) {
		ctx, cancel := db.governor.wrapContext(ctx)
		defer cancel()

		if len(params) == 0 {
			return db.executeCypher(ctx, pq.ast)
		}

		// Deep-copy and resolve parameters so the cached AST is not mutated.
		resolved := *pq.ast
		if err := resolveParams(&resolved, params); err != nil {
			return nil, err
		}
		return db.executeCypher(ctx, &resolved)
	})
}

// QueryCacheStats returns statistics about the query plan cache.
func (db *DB) QueryCacheStats() CacheStats {
	return db.cache.stats()
}
