package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/gritive/GrainFS/internal/chunkref"
)

const (
	appendSummaryPrefix = "appsum:"
	appendSegmentPrefix = "appseg:"
	appendSideSeqWidth  = 20
)

type appendSummary struct {
	Size            int64
	SegmentCount    int
	ETagPartCount   int
	ETagDigestState []byte
}

type AppendSummary = appendSummary

func appendSummaryKey(bucket, key, versionID string) []byte {
	return []byte(appendSummaryPrefix + bucket + "/" + key + "/" + versionID)
}

func appendSegmentKey(bucket, key, versionID string, seq int) []byte {
	return []byte(fmt.Sprintf("%s%s/%s/%s/%0*d", appendSegmentPrefix, bucket, key, versionID, appendSideSeqWidth, seq))
}

func encodeAppendSummary(s appendSummary) []byte { //nolint:unused // referenced by append_side_record_test.go until the writer path lands.
	if s.ETagPartCount == 0 && len(s.ETagDigestState) == 0 {
		var buf [16]byte
		binary.BigEndian.PutUint64(buf[0:8], uint64(s.Size))
		binary.BigEndian.PutUint64(buf[8:16], uint64(s.SegmentCount))
		return buf[:]
	}
	buf := make([]byte, 28+len(s.ETagDigestState))
	binary.BigEndian.PutUint64(buf[0:8], uint64(s.Size))
	binary.BigEndian.PutUint64(buf[8:16], uint64(s.SegmentCount))
	binary.BigEndian.PutUint64(buf[16:24], uint64(s.ETagPartCount))
	binary.BigEndian.PutUint32(buf[24:28], uint32(len(s.ETagDigestState)))
	copy(buf[28:], s.ETagDigestState)
	return buf
}

func EncodeAppendSummary(s AppendSummary) []byte {
	return encodeAppendSummary(s)
}

func decodeAppendSummary(data []byte) (appendSummary, error) {
	if len(data) != 16 && len(data) < 28 {
		return appendSummary{}, fmt.Errorf("append summary: invalid length %d", len(data))
	}
	summary := appendSummary{
		Size:         int64(binary.BigEndian.Uint64(data[0:8])),
		SegmentCount: int(binary.BigEndian.Uint64(data[8:16])),
	}
	if len(data) == 16 {
		return summary, nil
	}
	stateLen := int(binary.BigEndian.Uint32(data[24:28]))
	if len(data) != 28+stateLen {
		return appendSummary{}, fmt.Errorf("append summary: invalid etag state length %d for %d bytes", stateLen, len(data))
	}
	summary.ETagPartCount = int(binary.BigEndian.Uint64(data[16:24]))
	summary.ETagDigestState = append([]byte(nil), data[28:]...)
	return summary, nil
}

func DecodeAppendSummary(data []byte) (AppendSummary, error) {
	return decodeAppendSummary(data)
}

func encodeAppendSegment(seg SegmentRef) []byte { //nolint:unused // referenced by append_side_record_test.go until the writer path lands.
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

func (b *LocalBackend) readAppendSummaryInTxn(txn *badger.Txn, bucket, key, versionID string) (appendSummary, error) {
	item, err := txn.Get(appendSummaryKey(bucket, key, versionID))
	if err != nil {
		return appendSummary{}, err
	}
	var summary appendSummary
	if err := item.Value(func(v []byte) error {
		var derr error
		summary, derr = decodeAppendSummary(v)
		return derr
	}); err != nil {
		return appendSummary{}, err
	}
	return summary, nil
}

func (b *LocalBackend) writeAppendSideRecordsInTxn(txn *badger.Txn, bucket, key, versionID string, summary appendSummary, segments []SegmentRef) error {
	if err := txn.Set(appendSummaryKey(bucket, key, versionID), encodeAppendSummary(summary)); err != nil {
		return err
	}
	store := NewChunkRefStore(txn)
	m := chunkref.ObjectVersionID(bucket, key, versionID)
	for i, seg := range segments {
		if err := txn.Set(appendSegmentKey(bucket, key, versionID, i+1), encodeAppendSegment(seg)); err != nil {
			return err
		}
		if err := store.AddRef(m, chunkref.ChunkID(ParseLocator(seg.BlobID).String())); err != nil {
			return err
		}
	}
	return nil
}

func (b *LocalBackend) putAppendSideRecordAppend(ctx context.Context, bucket, key string, obj *Object, summary appendSummary, seg SegmentRef) error {
	_ = ctx
	return b.db.Update(func(txn *badger.Txn) error {
		record := *obj
		record.Segments = nil
		record.AppendCallMD5s = nil
		data, err := marshalObject(&record)
		if err != nil {
			return err
		}
		if err := setBadgerValue(txn, b.objectMetaKey(bucket, key), data); err != nil {
			return err
		}
		if err := txn.Set(appendSummaryKey(bucket, key, obj.VersionID), encodeAppendSummary(summary)); err != nil {
			return err
		}
		if err := txn.Set(appendSegmentKey(bucket, key, obj.VersionID, summary.SegmentCount), encodeAppendSegment(seg)); err != nil {
			return err
		}
		store := NewChunkRefStore(txn)
		return store.AddRef(chunkref.ObjectVersionID(bucket, key, obj.VersionID), chunkref.ChunkID(ParseLocator(seg.BlobID).String()))
	})
}

func (b *LocalBackend) putAppendSideRecordObject(ctx context.Context, bucket, key string, obj *Object, summary appendSummary, segments []SegmentRef) error {
	_ = ctx
	return b.db.Update(func(txn *badger.Txn) error {
		record := *obj
		record.Segments = nil
		record.AppendCallMD5s = nil
		if err := b.PutObjectRecordInTxn(txn, bucket, key, &record); err != nil {
			return err
		}
		return b.writeAppendSideRecordsInTxn(txn, bucket, key, obj.VersionID, summary, segments)
	})
}

func (b *LocalBackend) deleteAppendSideRecordsInTxn(txn *badger.Txn, bucket, key, versionID string, now time.Time) error {
	summary, err := b.readAppendSummaryInTxn(txn, bucket, key, versionID)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	store := NewChunkRefStore(txn)
	m := chunkref.ObjectVersionID(bucket, key, versionID)
	for seq := 1; seq <= summary.SegmentCount; seq++ {
		item, err := txn.Get(appendSegmentKey(bucket, key, versionID, seq))
		if err != nil {
			return err
		}
		var seg SegmentRef
		if err := item.Value(func(v []byte) error {
			var derr error
			seg, derr = decodeAppendSegment(v)
			return derr
		}); err != nil {
			return err
		}
		if err := store.RemoveRef(m, chunkref.ChunkID(ParseLocator(seg.BlobID).String()), now); err != nil {
			return err
		}
		if err := txn.Delete(appendSegmentKey(bucket, key, versionID, seq)); err != nil {
			return err
		}
	}
	return txn.Delete(appendSummaryKey(bucket, key, versionID))
}

func (b *LocalBackend) loadAppendSideSegmentsInTxn(txn *badger.Txn, bucket, key string, obj *Object) error {
	summary, err := b.readAppendSummaryInTxn(txn, bucket, key, obj.VersionID)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return fmt.Errorf("append side summary missing for %s/%s", bucket, key)
		}
		return err
	}
	if summary.Size != obj.Size {
		return fmt.Errorf("append side summary size %d does not match object size %d", summary.Size, obj.Size)
	}
	segments := make([]SegmentRef, 0, summary.SegmentCount)
	var total int64
	for seq := 1; seq <= summary.SegmentCount; seq++ {
		item, err := txn.Get(appendSegmentKey(bucket, key, obj.VersionID, seq))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return fmt.Errorf("append side segment %d missing for %s/%s", seq, bucket, key)
			}
			return err
		}
		var seg SegmentRef
		if err := item.Value(func(v []byte) error {
			var derr error
			seg, derr = decodeAppendSegment(v)
			return derr
		}); err != nil {
			return err
		}
		total += seg.Size
		segments = append(segments, seg)
	}
	if total != obj.Size {
		return fmt.Errorf("append side segment size %d does not match object size %d", total, obj.Size)
	}
	obj.Segments = segments
	return nil
}
