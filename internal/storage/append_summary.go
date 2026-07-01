package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type appendSummary struct {
	Size         int64
	SegmentCount int
	// CompactedPrefixCount is the count of earlier append side segments that were
	// consumed into Coalesced refs. Live tail segments start at this prefix + 1.
	CompactedPrefixCount int
	ETagPartCount        int
	ETagDigestState      []byte
}

type AppendSummary = appendSummary

func appendSummaryLogicalAppendCount(summary appendSummary) int {
	count := summary.CompactedPrefixCount + summary.SegmentCount
	if summary.ETagPartCount > count {
		return summary.ETagPartCount
	}
	return count
}

func AppendSummaryLogicalAppendCount(summary AppendSummary) int {
	return appendSummaryLogicalAppendCount(summary)
}

func encodeAppendSummary(s appendSummary) []byte {
	if s.ETagPartCount == 0 && len(s.ETagDigestState) == 0 && s.CompactedPrefixCount == 0 {
		var buf [16]byte
		binary.BigEndian.PutUint64(buf[0:8], uint64(s.Size))
		binary.BigEndian.PutUint64(buf[8:16], uint64(s.SegmentCount))
		return buf[:]
	}
	if s.ETagPartCount == 0 && len(s.ETagDigestState) == 0 {
		var buf [24]byte
		binary.BigEndian.PutUint64(buf[0:8], uint64(s.Size))
		binary.BigEndian.PutUint64(buf[8:16], uint64(s.SegmentCount))
		binary.BigEndian.PutUint64(buf[16:24], uint64(s.CompactedPrefixCount))
		return buf[:]
	}
	if s.CompactedPrefixCount == 0 {
		buf := make([]byte, 28+len(s.ETagDigestState))
		binary.BigEndian.PutUint64(buf[0:8], uint64(s.Size))
		binary.BigEndian.PutUint64(buf[8:16], uint64(s.SegmentCount))
		binary.BigEndian.PutUint64(buf[16:24], uint64(s.ETagPartCount))
		binary.BigEndian.PutUint32(buf[24:28], uint32(len(s.ETagDigestState)))
		copy(buf[28:], s.ETagDigestState)
		return buf
	}
	buf := make([]byte, 36+len(s.ETagDigestState))
	binary.BigEndian.PutUint64(buf[0:8], uint64(s.Size))
	binary.BigEndian.PutUint64(buf[8:16], uint64(s.SegmentCount))
	binary.BigEndian.PutUint64(buf[16:24], uint64(s.ETagPartCount))
	binary.BigEndian.PutUint32(buf[24:28], uint32(len(s.ETagDigestState)))
	copy(buf[28:], s.ETagDigestState)
	binary.BigEndian.PutUint64(buf[28+len(s.ETagDigestState):36+len(s.ETagDigestState)], uint64(s.CompactedPrefixCount))
	return buf
}

func EncodeAppendSummary(s AppendSummary) []byte {
	return encodeAppendSummary(s)
}

func decodeAppendSummary(data []byte) (appendSummary, error) {
	if len(data) != 16 && len(data) != 24 && len(data) < 28 {
		return appendSummary{}, fmt.Errorf("append summary: invalid length %d", len(data))
	}
	summary := appendSummary{
		Size:         int64(binary.BigEndian.Uint64(data[0:8])),
		SegmentCount: int(binary.BigEndian.Uint64(data[8:16])),
	}
	if len(data) == 16 {
		return summary, nil
	}
	if len(data) == 24 {
		summary.CompactedPrefixCount = int(binary.BigEndian.Uint64(data[16:24]))
		return summary, nil
	}
	stateLen := int(binary.BigEndian.Uint32(data[24:28]))
	if len(data) != 28+stateLen && len(data) != 36+stateLen {
		return appendSummary{}, fmt.Errorf("append summary: invalid etag state length %d for %d bytes", stateLen, len(data))
	}
	summary.ETagPartCount = int(binary.BigEndian.Uint64(data[16:24]))
	summary.ETagDigestState = append([]byte(nil), data[28:]...)
	if len(data) == 36+stateLen {
		summary.ETagDigestState = append([]byte(nil), data[28:28+stateLen]...)
		summary.CompactedPrefixCount = int(binary.BigEndian.Uint64(data[28+stateLen : 36+stateLen]))
	}
	return summary, nil
}

func DecodeAppendSummary(data []byte) (AppendSummary, error) {
	return decodeAppendSummary(data)
}

func encodeAppendSegment(seg SegmentRef) []byte {
	var buf bytes.Buffer
	writeString := func(s string) {
		_ = binary.Write(&buf, binary.BigEndian, uint32(len(s)))
		buf.WriteString(s)
	}
	writeBytes := func(b []byte) {
		_ = binary.Write(&buf, binary.BigEndian, uint32(len(b)))
		buf.Write(b)
	}
	writeString(seg.BlobID)
	_ = binary.Write(&buf, binary.BigEndian, seg.Size)
	writeBytes(seg.Checksum)
	writeString(seg.PlacementGroupID)
	_ = binary.Write(&buf, binary.BigEndian, seg.ShardSize)
	_ = binary.Write(&buf, binary.BigEndian, seg.ECData)
	_ = binary.Write(&buf, binary.BigEndian, seg.ECParity)
	_ = binary.Write(&buf, binary.BigEndian, seg.StripeBytes)
	_ = binary.Write(&buf, binary.BigEndian, uint32(len(seg.NodeIDs)))
	for _, nodeID := range seg.NodeIDs {
		writeString(nodeID)
	}
	return buf.Bytes()
}

func EncodeAppendSegment(seg SegmentRef) []byte {
	return encodeAppendSegment(seg)
}

func decodeAppendSegment(data []byte) (SegmentRef, error) {
	r := bytes.NewReader(data)
	readString := func() (string, error) {
		var n uint32
		if err := binary.Read(r, binary.BigEndian, &n); err != nil {
			return "", err
		}
		if uint64(n) > uint64(r.Len()) {
			return "", io.ErrUnexpectedEOF
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", err
		}
		return string(buf), nil
	}
	readBytes := func() ([]byte, error) {
		var n uint32
		if err := binary.Read(r, binary.BigEndian, &n); err != nil {
			return nil, err
		}
		if uint64(n) > uint64(r.Len()) {
			return nil, io.ErrUnexpectedEOF
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return buf, nil
	}
	blobID, err := readString()
	if err != nil {
		return SegmentRef{}, fmt.Errorf("append segment: blob id: %w", err)
	}
	var size int64
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return SegmentRef{}, fmt.Errorf("append segment: size: %w", err)
	}
	checksum, err := readBytes()
	if err != nil {
		return SegmentRef{}, fmt.Errorf("append segment: checksum: %w", err)
	}
	placementGroupID, err := readString()
	if err != nil {
		return SegmentRef{}, fmt.Errorf("append segment: placement group: %w", err)
	}
	var shardSize int32
	if err := binary.Read(r, binary.BigEndian, &shardSize); err != nil {
		return SegmentRef{}, fmt.Errorf("append segment: shard size: %w", err)
	}
	var ecData uint8
	if err := binary.Read(r, binary.BigEndian, &ecData); err != nil {
		return SegmentRef{}, fmt.Errorf("append segment: ec data: %w", err)
	}
	var ecParity uint8
	if err := binary.Read(r, binary.BigEndian, &ecParity); err != nil {
		return SegmentRef{}, fmt.Errorf("append segment: ec parity: %w", err)
	}
	var stripeBytes uint32
	if err := binary.Read(r, binary.BigEndian, &stripeBytes); err != nil {
		return SegmentRef{}, fmt.Errorf("append segment: stripe bytes: %w", err)
	}
	var nodeCount uint32
	if err := binary.Read(r, binary.BigEndian, &nodeCount); err != nil {
		return SegmentRef{}, fmt.Errorf("append segment: node count: %w", err)
	}
	nodeIDs := make([]string, 0, nodeCount)
	for i := uint32(0); i < nodeCount; i++ {
		nodeID, err := readString()
		if err != nil {
			return SegmentRef{}, fmt.Errorf("append segment: node id %d: %w", i, err)
		}
		nodeIDs = append(nodeIDs, nodeID)
	}
	if r.Len() != 0 {
		return SegmentRef{}, fmt.Errorf("append segment: trailing bytes %d", r.Len())
	}
	return SegmentRef{
		BlobID:           blobID,
		Size:             size,
		Checksum:         checksum,
		PlacementGroupID: placementGroupID,
		ShardSize:        shardSize,
		ECData:           ecData,
		ECParity:         ecParity,
		StripeBytes:      stripeBytes,
		NodeIDs:          nodeIDs,
	}, nil
}

func DecodeAppendSegment(data []byte) (SegmentRef, error) {
	return decodeAppendSegment(data)
}
