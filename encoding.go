package graphdb

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

// Key encoding helpers for bbolt.
// All integer keys use big-endian encoding for proper byte-ordering in B+tree.

// encodeUint64 encodes a uint64 as 8-byte big-endian.
func encodeUint64(v uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, v)
	return buf
}

// decodeUint64 decodes 8-byte big-endian to uint64.
func decodeUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// encodeNodeID encodes a NodeID as 8-byte big-endian key.
func encodeNodeID(id NodeID) []byte {
	return encodeUint64(uint64(id))
}

// decodeNodeID decodes an 8-byte key to NodeID.
func decodeNodeID(b []byte) NodeID {
	return NodeID(decodeUint64(b))
}

// encodeEdgeID encodes an EdgeID as 8-byte big-endian key.
func encodeEdgeID(id EdgeID) []byte {
	return encodeUint64(uint64(id))
}

// decodeEdgeID decodes an 8-byte key to EdgeID.
func decodeEdgeID(b []byte) EdgeID {
	return EdgeID(decodeUint64(b))
}

// encodeAdjKey creates a composite adjacency key: nodeID(8) + edgeID(8).
// This allows efficient prefix scans for all edges of a given node.
func encodeAdjKey(nodeID NodeID, edgeID EdgeID) []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[:8], uint64(nodeID))
	binary.BigEndian.PutUint64(buf[8:], uint64(edgeID))
	return buf
}

// decodeAdjKey decodes a 16-byte adjacency key into nodeID and edgeID.
func decodeAdjKey(b []byte) (NodeID, EdgeID) {
	nodeID := NodeID(binary.BigEndian.Uint64(b[:8]))
	edgeID := EdgeID(binary.BigEndian.Uint64(b[8:]))
	return nodeID, edgeID
}

// encodeAdjValue encodes an adjacency value as binary: nodeID(8) + label bytes.
// This is much faster than JSON — no allocs for label when decoding.
func encodeAdjValue(targetID NodeID, label string) []byte {
	buf := make([]byte, 8+len(label))
	binary.BigEndian.PutUint64(buf[:8], uint64(targetID))
	copy(buf[8:], label)
	return buf
}

// decodeAdjValue decodes a binary adjacency value.
func decodeAdjValue(data []byte) (NodeID, string) {
	if len(data) < 8 {
		return 0, ""
	}
	id := NodeID(binary.BigEndian.Uint64(data[:8]))
	label := string(data[8:])
	return id, label
}

// encodeIndexKey creates a composite index key: prefix + ID.
// Used for label->nodeID and edgeType->edgeID indexes.
func encodeIndexKey(prefix string, id uint64) []byte {
	prefixBytes := []byte(prefix)
	buf := make([]byte, len(prefixBytes)+1+8) // prefix + separator + id
	copy(buf, prefixBytes)
	buf[len(prefixBytes)] = 0x00 // null separator
	binary.BigEndian.PutUint64(buf[len(prefixBytes)+1:], id)
	return buf
}

// encodeIndexPrefix creates just the prefix part of an index key for scanning.
func encodeIndexPrefix(prefix string) []byte {
	prefixBytes := []byte(prefix)
	buf := make([]byte, len(prefixBytes)+1) // prefix + separator
	copy(buf, prefixBytes)
	buf[len(prefixBytes)] = 0x00
	return buf
}

// propsMagic is a 1-byte prefix that distinguishes MessagePack-encoded
// properties from legacy JSON-encoded properties. Any data that does NOT
// start with this byte is assumed to be JSON (backward compatibility).
const propsMagic byte = 0x01

// encodeProps serializes properties to MessagePack with a magic-byte prefix.
func encodeProps(props Props) ([]byte, error) {
	if props == nil {
		props = make(Props)
	}
	raw, err := msgpack.Marshal(props)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 1+len(raw))
	buf[0] = propsMagic
	copy(buf[1:], raw)
	return buf, nil
}

// decodeProps deserializes properties. It auto-detects the format:
//   - If the first byte is propsMagic → MessagePack (new format)
//   - Otherwise → JSON (legacy data written before the encoding upgrade)
func decodeProps(data []byte) (Props, error) {
	var props Props
	if len(data) > 0 && data[0] == propsMagic {
		if err := msgpack.Unmarshal(data[1:], &props); err != nil {
			return nil, err
		}
	} else {
		if err := json.Unmarshal(data, &props); err != nil {
			return nil, err
		}
	}
	if props == nil {
		props = make(Props)
	}
	return props, nil
}

// encodeEdge serializes an edge to binary: ID(8) + From(8) + To(8) + labelLen(4) + label + propsMsgpack.
// Fixed-size header avoids serialization overhead for the hot fields.
func encodeEdge(e *Edge) ([]byte, error) {
	labelBytes := []byte(e.Label)
	propsData, err := encodeProps(e.Props)
	if err != nil {
		return nil, err
	}

	// Header: 8 + 8 + 8 + 4 = 28 bytes fixed, then label, then props JSON.
	buf := make([]byte, 28+len(labelBytes)+len(propsData))
	binary.BigEndian.PutUint64(buf[0:8], uint64(e.ID))
	binary.BigEndian.PutUint64(buf[8:16], uint64(e.From))
	binary.BigEndian.PutUint64(buf[16:24], uint64(e.To))
	binary.BigEndian.PutUint32(buf[24:28], uint32(len(labelBytes)))
	copy(buf[28:28+len(labelBytes)], labelBytes)
	copy(buf[28+len(labelBytes):], propsData)
	return buf, nil
}

// decodeEdge deserializes a binary edge.
func decodeEdge(data []byte) (*Edge, error) {
	if len(data) < 28 {
		return nil, fmt.Errorf("graphdb: edge data too short (%d bytes)", len(data))
	}
	e := &Edge{
		ID:   EdgeID(binary.BigEndian.Uint64(data[0:8])),
		From: NodeID(binary.BigEndian.Uint64(data[8:16])),
		To:   NodeID(binary.BigEndian.Uint64(data[16:24])),
	}
	labelLen := int(binary.BigEndian.Uint32(data[24:28]))
	if 28+labelLen > len(data) {
		return nil, fmt.Errorf("graphdb: edge data corrupted")
	}
	e.Label = string(data[28 : 28+labelLen])
	propsData := data[28+labelLen:]
	if len(propsData) > 0 {
		props, err := decodeProps(propsData)
		if err != nil {
			return nil, err
		}
		e.Props = props
	}
	return e, nil
}
