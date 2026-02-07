package graphdb

import (
	"sync"
)

// nodeCache is a sharded LRU cache for hot Node objects.
// It reduces bbolt B-tree lookups and msgpack decoding for frequently
// accessed nodes. Each shard has its own mutex for minimal contention.
//
// Cache entries store deep copies of Node objects so mutations to the
// returned value do not corrupt the cache.
type nodeCache struct {
	shards    []cacheShard
	shardMask uint64
	capacity  int // total capacity across all shards
}

// cacheShard is a single LRU shard protected by its own mutex.
type cacheShard struct {
	mu       sync.Mutex
	items    map[NodeID]*lruEntry
	head     *lruEntry // most recently used
	tail     *lruEntry // least recently used
	capacity int
}

// lruEntry is a doubly-linked list node in the LRU chain.
type lruEntry struct {
	key  NodeID
	node *Node
	prev *lruEntry
	next *lruEntry
}

const cacheShardCount = 16

// newNodeCache creates a new sharded LRU cache with the given total capacity.
// If capacity <= 0, the cache is a no-op (all methods are safe to call).
func newNodeCache(capacity int) *nodeCache {
	nc := &nodeCache{
		shards:    make([]cacheShard, cacheShardCount),
		shardMask: uint64(cacheShardCount - 1),
		capacity:  capacity,
	}
	perShard := capacity / cacheShardCount
	if perShard < 1 {
		perShard = 1
	}
	for i := range nc.shards {
		nc.shards[i] = cacheShard{
			items:    make(map[NodeID]*lruEntry),
			capacity: perShard,
		}
	}
	return nc
}

// shard returns the cache shard for the given node ID.
func (nc *nodeCache) shard(id NodeID) *cacheShard {
	return &nc.shards[uint64(id)&nc.shardMask]
}

// Get retrieves a node from the cache. Returns nil if not found or if
// the cache is disabled (capacity <= 0). The returned Node is a deep copy.
func (nc *nodeCache) Get(id NodeID) *Node {
	if nc.capacity <= 0 {
		return nil
	}
	s := nc.shard(id)
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.items[id]
	if !ok {
		return nil
	}
	s.moveToFront(entry)
	return copyNode(entry.node)
}

// Put adds or updates a node in the cache. The node is deep-copied before
// storage so external mutations don't corrupt the cache.
func (nc *nodeCache) Put(node *Node) {
	if nc.capacity <= 0 || node == nil {
		return
	}
	s := nc.shard(node.ID)
	s.mu.Lock()
	defer s.mu.Unlock()

	if entry, ok := s.items[node.ID]; ok {
		// Update existing entry.
		entry.node = copyNode(node)
		s.moveToFront(entry)
		return
	}

	// Insert new entry.
	entry := &lruEntry{key: node.ID, node: copyNode(node)}
	s.items[node.ID] = entry
	s.pushFront(entry)

	// Evict if over capacity.
	if len(s.items) > s.capacity {
		s.evictLRU()
	}
}

// Invalidate removes a node from the cache.
func (nc *nodeCache) Invalidate(id NodeID) {
	if nc.capacity <= 0 {
		return
	}
	s := nc.shard(id)
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.items[id]
	if !ok {
		return
	}
	s.removeEntry(entry)
	delete(s.items, id)
}

// Stats returns cache hit/miss statistics (for observability).
func (nc *nodeCache) Len() int {
	if nc.capacity <= 0 {
		return 0
	}
	total := 0
	for i := range nc.shards {
		nc.shards[i].mu.Lock()
		total += len(nc.shards[i].items)
		nc.shards[i].mu.Unlock()
	}
	return total
}

// ---------------------------------------------------------------------------
// LRU doubly-linked list operations (caller must hold shard lock)
// ---------------------------------------------------------------------------

func (s *cacheShard) moveToFront(e *lruEntry) {
	if s.head == e {
		return
	}
	s.removeEntry(e)
	s.pushFront(e)
}

func (s *cacheShard) pushFront(e *lruEntry) {
	e.prev = nil
	e.next = s.head
	if s.head != nil {
		s.head.prev = e
	}
	s.head = e
	if s.tail == nil {
		s.tail = e
	}
}

func (s *cacheShard) removeEntry(e *lruEntry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		s.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		s.tail = e.prev
	}
	e.prev = nil
	e.next = nil
}

func (s *cacheShard) evictLRU() {
	if s.tail == nil {
		return
	}
	victim := s.tail
	s.removeEntry(victim)
	delete(s.items, victim.key)
}

// ---------------------------------------------------------------------------
// Deep-copy helpers
// ---------------------------------------------------------------------------

// copyNode returns a deep copy of a Node so cache entries are isolated.
func copyNode(n *Node) *Node {
	if n == nil {
		return nil
	}
	cp := &Node{ID: n.ID}

	if n.Labels != nil {
		cp.Labels = make([]string, len(n.Labels))
		copy(cp.Labels, n.Labels)
	}

	if n.Props != nil {
		cp.Props = make(Props, len(n.Props))
		for k, v := range n.Props {
			cp.Props[k] = v
		}
	}

	return cp
}
