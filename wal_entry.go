package graphdb

import (
	"github.com/vmihailenco/msgpack/v5"
)

// ---------------------------------------------------------------------------
// WAL Entry Types
//
// Each mutation to the graph (add node, add edge, update, delete, etc.) is
// recorded as a WAL entry before being applied to bbolt. The WAL is the
// source of truth for replication: followers replay entries to stay in sync.
//
// Entry payloads are encoded with msgpack for compact, fast serialization.
// Every entry includes the allocated IDs so followers can apply the exact
// same mutation without re-allocating (deterministic replay).
// ---------------------------------------------------------------------------

// OpType identifies the type of mutation recorded in a WAL entry.
type OpType uint8

const (
	OpAddNode                OpType = iota + 1 // single node creation
	OpAddNodeBatch                             // batch node creation
	OpUpdateNode                               // merge-update node properties
	OpSetNodeProps                             // full-replace node properties
	OpDeleteNode                               // node + connected edge deletion
	OpAddEdge                                  // single edge creation
	OpAddEdgeBatch                             // batch edge creation
	OpDeleteEdge                               // edge deletion
	OpUpdateEdge                               // merge-update edge properties
	OpAddNodeWithLabels                        // node creation with labels
	OpAddLabel                                 // add labels to existing node
	OpRemoveLabel                              // remove labels from existing node
	OpCreateIndex                              // create property index
	OpDropIndex                                // drop property index
	OpCreateCompositeIndex                     // create composite property index
	OpDropCompositeIndex                       // drop composite property index
	OpCreateUniqueConstraint                   // create unique constraint on (label, property)
	OpDropUniqueConstraint                     // drop unique constraint
)

// String returns a human-readable name for the operation type.
func (op OpType) String() string {
	switch op {
	case OpAddNode:
		return "AddNode"
	case OpAddNodeBatch:
		return "AddNodeBatch"
	case OpUpdateNode:
		return "UpdateNode"
	case OpSetNodeProps:
		return "SetNodeProps"
	case OpDeleteNode:
		return "DeleteNode"
	case OpAddEdge:
		return "AddEdge"
	case OpAddEdgeBatch:
		return "AddEdgeBatch"
	case OpDeleteEdge:
		return "DeleteEdge"
	case OpUpdateEdge:
		return "UpdateEdge"
	case OpAddNodeWithLabels:
		return "AddNodeWithLabels"
	case OpAddLabel:
		return "AddLabel"
	case OpRemoveLabel:
		return "RemoveLabel"
	case OpCreateIndex:
		return "CreateIndex"
	case OpDropIndex:
		return "DropIndex"
	case OpCreateCompositeIndex:
		return "CreateCompositeIndex"
	case OpDropCompositeIndex:
		return "DropCompositeIndex"
	case OpCreateUniqueConstraint:
		return "CreateUniqueConstraint"
	case OpDropUniqueConstraint:
		return "DropUniqueConstraint"
	default:
		return "Unknown"
	}
}

// WALEntry represents a single committed mutation in the write-ahead log.
// The LSN (Log Sequence Number) is a monotonically increasing counter that
// uniquely identifies each entry across the entire WAL history. Followers
// track their applied LSN to request only new entries from the leader.
type WALEntry struct {
	LSN       uint64 `msgpack:"lsn"`     // monotonically increasing sequence number
	Timestamp int64  `msgpack:"ts"`      // unix nanoseconds when the entry was created
	Op        OpType `msgpack:"op"`      // mutation type
	Payload   []byte `msgpack:"payload"` // msgpack-encoded operation arguments
}

// ---------------------------------------------------------------------------
// Payload structs — one per OpType.
//
// Each struct captures the exact arguments needed to replay the mutation
// on a follower. IDs are included so followers produce identical state.
// ---------------------------------------------------------------------------

// WALAddNode is the payload for OpAddNode.
type WALAddNode struct {
	ID    NodeID `msgpack:"id"`
	Props Props  `msgpack:"props"`
}

// WALAddNodeBatch is the payload for OpAddNodeBatch.
type WALAddNodeBatch struct {
	Nodes []WALBatchNode `msgpack:"nodes"`
}

// WALBatchNode is a single node within a batch add.
type WALBatchNode struct {
	ID    NodeID `msgpack:"id"`
	Props Props  `msgpack:"props"`
}

// WALUpdateNode is the payload for OpUpdateNode (merge update).
type WALUpdateNode struct {
	ID    NodeID `msgpack:"id"`
	Props Props  `msgpack:"props"` // properties to merge into existing
}

// WALSetNodeProps is the payload for OpSetNodeProps (full replace).
type WALSetNodeProps struct {
	ID    NodeID `msgpack:"id"`
	Props Props  `msgpack:"props"` // complete replacement properties
}

// WALDeleteNode is the payload for OpDeleteNode.
type WALDeleteNode struct {
	ID NodeID `msgpack:"id"`
}

// WALAddEdge is the payload for OpAddEdge.
type WALAddEdge struct {
	ID    EdgeID `msgpack:"id"`
	From  NodeID `msgpack:"from"`
	To    NodeID `msgpack:"to"`
	Label string `msgpack:"label"`
	Props Props  `msgpack:"props"`
}

// WALAddEdgeBatch is the payload for OpAddEdgeBatch.
type WALAddEdgeBatch struct {
	Edges []WALBatchEdge `msgpack:"edges"`
}

// WALBatchEdge is a single edge within a batch add.
type WALBatchEdge struct {
	ID    EdgeID `msgpack:"id"`
	From  NodeID `msgpack:"from"`
	To    NodeID `msgpack:"to"`
	Label string `msgpack:"label"`
	Props Props  `msgpack:"props"`
}

// WALDeleteEdge is the payload for OpDeleteEdge.
type WALDeleteEdge struct {
	ID    EdgeID `msgpack:"id"`
	From  NodeID `msgpack:"from"`
	To    NodeID `msgpack:"to"`
	Label string `msgpack:"label"`
}

// WALUpdateEdge is the payload for OpUpdateEdge.
type WALUpdateEdge struct {
	ID    EdgeID `msgpack:"id"`
	From  NodeID `msgpack:"from"` // needed to locate the edge's shard
	Props Props  `msgpack:"props"`
}

// WALAddNodeWithLabels is the payload for OpAddNodeWithLabels.
type WALAddNodeWithLabels struct {
	ID     NodeID   `msgpack:"id"`
	Labels []string `msgpack:"labels"`
	Props  Props    `msgpack:"props"`
}

// WALAddLabel is the payload for OpAddLabel.
type WALAddLabel struct {
	ID     NodeID   `msgpack:"id"`
	Labels []string `msgpack:"labels"`
}

// WALRemoveLabel is the payload for OpRemoveLabel.
type WALRemoveLabel struct {
	ID     NodeID   `msgpack:"id"`
	Labels []string `msgpack:"labels"`
}

// WALCreateIndex is the payload for OpCreateIndex.
type WALCreateIndex struct {
	PropName string `msgpack:"prop_name"`
}

// WALDropIndex is the payload for OpDropIndex.
type WALDropIndex struct {
	PropName string `msgpack:"prop_name"`
}

// WALCreateCompositeIndex is the payload for OpCreateCompositeIndex.
type WALCreateCompositeIndex struct {
	PropNames []string `msgpack:"prop_names"`
}

// WALDropCompositeIndex is the payload for OpDropCompositeIndex.
type WALDropCompositeIndex struct {
	PropNames []string `msgpack:"prop_names"`
}

// WALCreateUniqueConstraint is the payload for OpCreateUniqueConstraint.
type WALCreateUniqueConstraint struct {
	Label    string `msgpack:"label"`
	Property string `msgpack:"property"`
}

// WALDropUniqueConstraint is the payload for OpDropUniqueConstraint.
type WALDropUniqueConstraint struct {
	Label    string `msgpack:"label"`
	Property string `msgpack:"property"`
}

// ---------------------------------------------------------------------------
// Payload encoding/decoding helpers
// ---------------------------------------------------------------------------

// encodeWALPayload serializes a payload struct to msgpack bytes.
func encodeWALPayload(v any) ([]byte, error) {
	return msgpack.Marshal(v)
}

// decodeWALPayload deserializes msgpack bytes into a payload struct.
func decodeWALPayload(data []byte, v any) error {
	return msgpack.Unmarshal(data, v)
}
