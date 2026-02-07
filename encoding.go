package graphdb

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"

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

// Magic bytes for property encoding format detection.
const (
	propsMagic    byte = 0x01 // MessagePack without checksum (v1)
	propsMagicCRC byte = 0x02 // MessagePack with CRC32 checksum (v2)
)

// crc32Table is the precomputed Castagnoli CRC32 table (hardware-accelerated on modern CPUs).
var crc32Table = crc32.MakeTable(crc32.Castagnoli)

// encodeProps serializes properties to MessagePack with a CRC32 checksum.
// Format: magic(1) + msgpack_data + crc32(4)
func encodeProps(props Props) ([]byte, error) {
	if props == nil {
		props = make(Props)
	}
	raw, err := msgpack.Marshal(props)
	if err != nil {
		return nil, err
	}
	// magic(1) + data(N) + crc32(4)
	buf := make([]byte, 1+len(raw)+4)
	buf[0] = propsMagicCRC
	copy(buf[1:], raw)
	checksum := crc32.Checksum(buf[:1+len(raw)], crc32Table)
	binary.BigEndian.PutUint32(buf[1+len(raw):], checksum)
	return buf, nil
}

// decodeProps deserializes properties. It auto-detects the format:
//   - 0x02 → MessagePack with CRC32 checksum (current)
//   - 0x01 → MessagePack without checksum (legacy v1)
//   - Otherwise → JSON (legacy data written before the encoding upgrade)
func decodeProps(data []byte) (Props, error) {
	var props Props
	if len(data) > 0 && data[0] == propsMagicCRC {
		// CRC32-protected msgpack.
		if len(data) < 5 {
			return nil, fmt.Errorf("graphdb: props data too short for checksum")
		}
		payload := data[:len(data)-4]
		stored := binary.BigEndian.Uint32(data[len(data)-4:])
		actual := crc32.Checksum(payload, crc32Table)
		if stored != actual {
			return nil, fmt.Errorf("graphdb: props checksum mismatch (stored=%08x actual=%08x)", stored, actual)
		}
		if err := msgpack.Unmarshal(payload[1:], &props); err != nil {
			return nil, err
		}
	} else if len(data) > 0 && data[0] == propsMagic {
		// Legacy msgpack without checksum.
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

// encodeEdge serializes an edge to binary: ID(8) + From(8) + To(8) + labelLen(4) + label + props + crc32(4).
// Fixed-size header avoids serialization overhead for the hot fields.
func encodeEdge(e *Edge) ([]byte, error) {
	labelBytes := []byte(e.Label)
	propsData, err := encodeProps(e.Props)
	if err != nil {
		return nil, err
	}

	// Header: 8 + 8 + 8 + 4 = 28 bytes fixed, then label, then props, then crc32(4).
	payloadLen := 28 + len(labelBytes) + len(propsData)
	buf := make([]byte, payloadLen+4)
	binary.BigEndian.PutUint64(buf[0:8], uint64(e.ID))
	binary.BigEndian.PutUint64(buf[8:16], uint64(e.From))
	binary.BigEndian.PutUint64(buf[16:24], uint64(e.To))
	binary.BigEndian.PutUint32(buf[24:28], uint32(len(labelBytes)))
	copy(buf[28:28+len(labelBytes)], labelBytes)
	copy(buf[28+len(labelBytes):], propsData)
	checksum := crc32.Checksum(buf[:payloadLen], crc32Table)
	binary.BigEndian.PutUint32(buf[payloadLen:], checksum)
	return buf, nil
}

// decodeEdge deserializes a binary edge and verifies the CRC32 checksum.
// Backward compatible: if the CRC32 check fails, falls back to legacy decoding
// (data written before checksums were introduced).
func decodeEdge(data []byte) (*Edge, error) {
	if len(data) < 28 {
		return nil, fmt.Errorf("graphdb: edge data too short (%d bytes)", len(data))
	}

	// Try CRC32-verified decode first (new format: payload + 4-byte CRC).
	if len(data) >= 32 {
		payload := data[:len(data)-4]
		stored := binary.BigEndian.Uint32(data[len(data)-4:])
		actual := crc32.Checksum(payload, crc32Table)
		if stored == actual {
			return decodeEdgePayload(payload)
		}
	}

	// Fallback: legacy edge data without CRC32 trailer.
	return decodeEdgePayload(data)
}

// decodeEdgePayload decodes the raw edge payload (without CRC32 trailer).
func decodeEdgePayload(payload []byte) (*Edge, error) {
	if len(payload) < 28 {
		return nil, fmt.Errorf("graphdb: edge payload too short (%d bytes)", len(payload))
	}

	e := &Edge{
		ID:   EdgeID(binary.BigEndian.Uint64(payload[0:8])),
		From: NodeID(binary.BigEndian.Uint64(payload[8:16])),
		To:   NodeID(binary.BigEndian.Uint64(payload[16:24])),
	}
	labelLen := int(binary.BigEndian.Uint32(payload[24:28]))
	if 28+labelLen > len(payload) {
		return nil, fmt.Errorf("graphdb: edge data corrupted")
	}
	e.Label = string(payload[28 : 28+labelLen])
	propsData := payload[28+labelLen:]
	if len(propsData) > 0 {
		props, err := decodeProps(propsData)
		if err != nil {
			return nil, err
		}
		e.Props = props
	}
	return e, nil
}
