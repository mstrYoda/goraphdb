package graphdb

import (
	"sync"
	"unsafe"
)

// nodeCache is a sharded, byte-budgeted LRU cache for hot Node objects.
// It reduces bbolt B-tree lookups and msgpack decoding for frequently
// accessed nodes. Each shard has its own mutex for minimal contention.
//
// Eviction is driven by estimated memory usage, not entry count:
// nodes with large property maps consume proportionally more budget,
// giving predictable memory footprint regardless of node sizes.
//
// Cache entries store deep copies of Node objects so mutations to the
// returned value do not corrupt the cache.
type nodeCache struct {
	shards    []cacheShard
	shardMask uint64
	budget    int64 // total budget in bytes across all shards
}

// cacheShard is a single LRU shard protected by its own mutex.
type cacheShard struct {
	mu         sync.Mutex
	items      map[NodeID]*lruEntry
	head       *lruEntry // most recently used
	tail       *lruEntry // least recently used
	budget     int64     // per-shard byte budget
	totalBytes int64     // current estimated bytes used
}

// lruEntry is a doubly-linked list node in the LRU chain.
type lruEntry struct {
	key       NodeID
	node      *Node
	sizeBytes int64 // estimated memory footprint of this entry
	prev      *lruEntry
	next      *lruEntry
}

const cacheShardCount = 16

// newNodeCache creates a new sharded LRU cache with the given byte budget.
// If budget <= 0, the cache is a no-op (all methods are safe to call).
func newNodeCache(budget int64) *nodeCache {
	nc := &nodeCache{
		shards:    make([]cacheShard, cacheShardCount),
		shardMask: uint64(cacheShardCount - 1),
		budget:    budget,
	}
	perShard := budget / cacheShardCount
	if perShard < 1 {
		perShard = 1
	}
	for i := range nc.shards {
		nc.shards[i] = cacheShard{
			items:  make(map[NodeID]*lruEntry),
			budget: perShard,
		}
	}
	return nc
}

// shard returns the cache shard for the given node ID.
func (nc *nodeCache) shard(id NodeID) *cacheShard {
	return &nc.shards[uint64(id)&nc.shardMask]
}

// Get retrieves a node from the cache. Returns nil if not found or if
// the cache is disabled (budget <= 0). The returned Node is a deep copy.
func (nc *nodeCache) Get(id NodeID) *Node {
	if nc.budget <= 0 {
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
	if nc.budget <= 0 || node == nil {
		return
	}
	s := nc.shard(node.ID)
	s.mu.Lock()
	defer s.mu.Unlock()

	copied := copyNode(node)
	size := estimateNodeBytes(copied)

	if entry, ok := s.items[node.ID]; ok {
		// Update existing entry — adjust byte accounting.
		s.totalBytes += size - entry.sizeBytes
		entry.node = copied
		entry.sizeBytes = size
		s.moveToFront(entry)
		s.evictUntilBudget()
		return
	}

	// Insert new entry.
	entry := &lruEntry{key: node.ID, node: copied, sizeBytes: size}
	s.items[node.ID] = entry
	s.pushFront(entry)
	s.totalBytes += size

	// Evict LRU entries until within budget.
	s.evictUntilBudget()
}

// Invalidate removes a node from the cache.
func (nc *nodeCache) Invalidate(id NodeID) {
	if nc.budget <= 0 {
		return
	}
	s := nc.shard(id)
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.items[id]
	if !ok {
		return
	}
	s.totalBytes -= entry.sizeBytes
	s.removeEntry(entry)
	delete(s.items, id)
}

// Len returns the total number of cached entries.
func (nc *nodeCache) Len() int {
	if nc.budget <= 0 {
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

// TotalBytes returns the total estimated bytes used across all shards.
func (nc *nodeCache) TotalBytes() int64 {
	if nc.budget <= 0 {
		return 0
	}
	var total int64
	for i := range nc.shards {
		nc.shards[i].mu.Lock()
		total += nc.shards[i].totalBytes
		nc.shards[i].mu.Unlock()
	}
	return total
}

// BudgetBytes returns the total byte budget across all shards.
func (nc *nodeCache) BudgetBytes() int64 {
	return nc.budget
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

func (s *cacheShard) evictUntilBudget() {
	for s.totalBytes > s.budget && s.tail != nil {
		victim := s.tail
		s.removeEntry(victim)
		s.totalBytes -= victim.sizeBytes
		delete(s.items, victim.key)
	}
}

// ---------------------------------------------------------------------------
// Size estimation
// ---------------------------------------------------------------------------

// Overhead constants for size estimation.
const (
	nodeBaseSize  = int64(unsafe.Sizeof(Node{}))     // Node struct header
	entryOverhead = int64(unsafe.Sizeof(lruEntry{})) // LRU entry + pointers
	mapBucketSize = 64                               // conservative per-bucket overhead for Go maps
)

// estimateNodeBytes returns the approximate heap footprint of a cached Node
// plus its LRU entry overhead. The estimate is intentionally conservative.
func estimateNodeBytes(n *Node) int64 {
	size := nodeBaseSize + entryOverhead

	// Labels: slice header + string headers + string data.
	for _, l := range n.Labels {
		size += 16 + int64(len(l)) // string header (~16) + data
	}

	// Props: map overhead + per-entry (key + value).
	if len(n.Props) > 0 {
		size += mapBucketSize // base map overhead
		for k, v := range n.Props {
			size += 16 + int64(len(k)) // key: string header + data
			size += estimateValueBytes(v)
		}
	}

	return size
}

// estimateValueBytes returns a rough byte estimate for a property value.
func estimateValueBytes(v interface{}) int64 {
	switch val := v.(type) {
	case string:
		return 16 + int64(len(val))
	case []byte:
		return 24 + int64(len(val))
	case int, int64, uint64, float64, bool:
		return 8
	case []interface{}:
		var total int64 = 24 // slice header
		for _, elem := range val {
			total += estimateValueBytes(elem)
		}
		return total
	case map[string]interface{}:
		var total int64 = mapBucketSize
		for k, elem := range val {
			total += 16 + int64(len(k)) + estimateValueBytes(elem)
		}
		return total
	default:
		return 32 // conservative fallback
	}
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
