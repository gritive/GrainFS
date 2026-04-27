package transport

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// Phase 2 #2: QUIC 내부 통신 압축 검토 — measure compression ratio and CPU
// cost on representative payloads of every StreamType except StreamData
// (EC shards are random bytes, excluded by the design doc).
//
// Compression candidates picked to span the realistic size and entropy
// distribution we send today:
//
//	gossip_node_stats   ~100B  FlatBuffers, mostly numeric (1 string + 4 nums)
//	receipt_gossip      ~2KB   FlatBuffers, list of 50 UUIDs (high entropy)
//	receipt_json        ~1KB   text JSON (HealReceipt — what receipt_query returns)
//	put_object_meta     ~300B  FlatBuffers, single S3 PUT command record
//	raft_batch          ~30KB  100 × put_object_meta back-to-back, simulates an
//	                           AppendEntries batch the leader sends out
//	set_ring            ~6KB   FlatBuffers, 256 vnodes — periodic ring rebroadcast
//
// We compare zstd at default level (high ratio) against s2 (Snappy fork,
// goal is throughput). klauspost/compress is already in go.mod for both,
// so this evaluation does not pull in new deps.
//
// Decision rule baked into the design doc:
//   - If compressed size < 60% of original AND encode + decode latency
//     stays under "what 1 Gbps wire would have spent on the original bytes",
//     compression is a net win and goes into codec.
//   - If neither bound is met for a stream class, leave it uncompressed.

func samplePayloads() map[string][]byte {
	out := make(map[string][]byte)

	// 1) NodeStatsMsg gossip — small, numeric heavy.
	{
		b := flatbuffers.NewBuilder(64)
		nodeIDOff := b.CreateString("node-east-1a-7f3c9d12")
		clusterpb.NodeStatsMsgStart(b)
		clusterpb.NodeStatsMsgAddNodeId(b, nodeIDOff)
		clusterpb.NodeStatsMsgAddDiskUsedPct(b, 0.6234)
		clusterpb.NodeStatsMsgAddDiskAvailBytes(b, 421_338_624_000)
		clusterpb.NodeStatsMsgAddRequestsPerSec(b, 1247.5)
		clusterpb.NodeStatsMsgAddJoinedAt(b, time.Now().Add(-3*24*time.Hour).Unix())
		root := clusterpb.NodeStatsMsgEnd(b)
		b.Finish(root)
		out["gossip_node_stats"] = append([]byte(nil), b.FinishedBytes()...)
	}

	// 2) ReceiptGossipMsg — 50 UUIDs as the rolling window (Phase 16).
	{
		b := flatbuffers.NewBuilder(2048)
		nodeIDOff := b.CreateString("node-east-1a-7f3c9d12")
		ids := make([]flatbuffers.UOffsetT, 50)
		for i := range ids {
			ids[i] = b.CreateString(fmt.Sprintf("rcpt_%016x_%016x", uint64(i)*0x9e3779b97f4a7c15, uint64(i+7)*0x9e3779b97f4a7c15))
		}
		clusterpb.ReceiptGossipMsgStartReceiptIdsVector(b, len(ids))
		for i := len(ids) - 1; i >= 0; i-- {
			b.PrependUOffsetT(ids[i])
		}
		idsVec := b.EndVector(len(ids))
		clusterpb.ReceiptGossipMsgStart(b)
		clusterpb.ReceiptGossipMsgAddNodeId(b, nodeIDOff)
		clusterpb.ReceiptGossipMsgAddReceiptIds(b, idsVec)
		root := clusterpb.ReceiptGossipMsgEnd(b)
		b.Finish(root)
		out["receipt_gossip"] = append([]byte(nil), b.FinishedBytes()...)
	}

	// 3) HealReceipt JSON — text payload returned by ReceiptQueryResponseMsg.
	{
		type objRef struct {
			Bucket    string `json:"bucket"`
			Key       string `json:"key"`
			VersionID string `json:"version_id,omitempty"`
		}
		type rcpt struct {
			ReceiptID     string    `json:"receipt_id"`
			KeyID         string    `json:"key_id"`
			Timestamp     time.Time `json:"timestamp"`
			Object        objRef    `json:"object"`
			ShardsLost    []int32   `json:"shards_lost"`
			ShardsRebuilt []int32   `json:"shards_rebuilt"`
			ParityUsed    int       `json:"parity_used"`
			PeersInvolved []string  `json:"peers_involved"`
			DurationMs    uint32    `json:"duration_ms"`
			EventIDs      []string  `json:"event_ids"`
			CorrelationID string    `json:"correlation_id,omitempty"`
		}
		r := rcpt{
			ReceiptID:     "rcpt_a1b2c3d4e5f60718_2934857619203847",
			KeyID:         "primary-2026-04",
			Timestamp:     time.Now().UTC(),
			Object:        objRef{Bucket: "production-backups", Key: "tenants/acme/2026-04-28/snapshot.tar.zst"},
			ShardsLost:    []int32{2, 5},
			ShardsRebuilt: []int32{2, 5},
			ParityUsed:    2,
			PeersInvolved: []string{"node-east-1a-7f3c9d12", "node-east-1b-44ab10c3", "node-east-1c-9982ee54"},
			DurationMs:    8472,
			EventIDs:      []string{"evt_01HW3ABCD1", "evt_01HW3ABCD2", "evt_01HW3ABCD3", "evt_01HW3ABCD4"},
			CorrelationID: "corr_2026-04-28T12:14:33Z_node-east-1a",
		}
		raw, _ := json.Marshal(r)
		out["receipt_json"] = raw
	}

	// 4) PutObjectMetaCmd — single S3 PUT record. Real workload sends
	//    these constantly through Raft replication.
	{
		out["put_object_meta"] = makePutObjectMeta(0)
	}

	// 5) raft_batch — 100 PutObjectMetaCmd records concatenated into
	//    one buffer to simulate an AppendEntries batch on a busy leader.
	//    Length-prefix each record so it survives transport framing.
	{
		var bigBuf []byte
		for i := 0; i < 100; i++ {
			rec := makePutObjectMeta(i)
			lenPrefix := []byte{byte(len(rec) >> 24), byte(len(rec) >> 16), byte(len(rec) >> 8), byte(len(rec))}
			bigBuf = append(bigBuf, lenPrefix...)
			bigBuf = append(bigBuf, rec...)
		}
		out["raft_batch"] = bigBuf
	}

	// 6) SetRingCmd — 256 vnodes. The ring is rebroadcast on membership
	//    change and on snapshot install.
	{
		b := flatbuffers.NewBuilder(8192)
		const vnodeCount = 256
		const nodeCount = 8
		nodeIDs := make([]flatbuffers.UOffsetT, nodeCount)
		for i := 0; i < nodeCount; i++ {
			nodeIDs[i] = b.CreateString(fmt.Sprintf("node-east-1%c-%016x", 'a'+byte(i%3), uint64(i)*0x9e3779b97f4a7c15))
		}
		entries := make([]flatbuffers.UOffsetT, vnodeCount)
		for i := 0; i < vnodeCount; i++ {
			clusterpb.VNodeEntryStart(b)
			clusterpb.VNodeEntryAddToken(b, uint32(i)*0x01010101)
			clusterpb.VNodeEntryAddNodeId(b, nodeIDs[i%nodeCount])
			entries[i] = clusterpb.VNodeEntryEnd(b)
		}
		clusterpb.SetRingCmdStartVnodesVector(b, len(entries))
		for i := len(entries) - 1; i >= 0; i-- {
			b.PrependUOffsetT(entries[i])
		}
		entriesVec := b.EndVector(len(entries))
		clusterpb.SetRingCmdStart(b)
		clusterpb.SetRingCmdAddVersion(b, 42)
		clusterpb.SetRingCmdAddVnodes(b, entriesVec)
		clusterpb.SetRingCmdAddVperNode(b, vnodeCount/nodeCount)
		root := clusterpb.SetRingCmdEnd(b)
		b.Finish(root)
		out["set_ring"] = append([]byte(nil), b.FinishedBytes()...)
	}

	return out
}

func makePutObjectMeta(i int) []byte {
	b := flatbuffers.NewBuilder(256)
	bucketOff := b.CreateString("production-backups")
	keyOff := b.CreateString(fmt.Sprintf("tenants/acme/2026-04-28/object-%08d.bin", i))
	contentTypeOff := b.CreateString("application/octet-stream")
	etagOff := b.CreateString(fmt.Sprintf("\"%032x\"", uint64(i)*0x9e3779b97f4a7c15))
	versionOff := b.CreateString("null")
	nodeOffs := []flatbuffers.UOffsetT{
		b.CreateString("node-east-1a-7f3c9d12"),
		b.CreateString("node-east-1b-44ab10c3"),
		b.CreateString("node-east-1c-9982ee54"),
		b.CreateString("node-east-1a-7f3c9d12"),
		b.CreateString("node-east-1b-44ab10c3"),
		b.CreateString("node-east-1c-9982ee54"),
	}
	clusterpb.PutObjectMetaCmdStartNodeIdsVector(b, len(nodeOffs))
	for j := len(nodeOffs) - 1; j >= 0; j-- {
		b.PrependUOffsetT(nodeOffs[j])
	}
	nodesVec := b.EndVector(len(nodeOffs))
	clusterpb.PutObjectMetaCmdStart(b)
	clusterpb.PutObjectMetaCmdAddBucket(b, bucketOff)
	clusterpb.PutObjectMetaCmdAddKey(b, keyOff)
	clusterpb.PutObjectMetaCmdAddSize(b, 1024*1024*4)
	clusterpb.PutObjectMetaCmdAddContentType(b, contentTypeOff)
	clusterpb.PutObjectMetaCmdAddEtag(b, etagOff)
	clusterpb.PutObjectMetaCmdAddModTime(b, time.Now().Unix())
	clusterpb.PutObjectMetaCmdAddVersionId(b, versionOff)
	clusterpb.PutObjectMetaCmdAddRingVersion(b, 42)
	clusterpb.PutObjectMetaCmdAddEcData(b, 4)
	clusterpb.PutObjectMetaCmdAddEcParity(b, 2)
	clusterpb.PutObjectMetaCmdAddNodeIds(b, nodesVec)
	root := clusterpb.PutObjectMetaCmdEnd(b)
	b.Finish(root)
	return append([]byte(nil), b.FinishedBytes()...)
}

// TestCompressionRatios runs once and prints a table — this is the
// data the design doc cites. Not a benchmark; ratios alone don't need
// the b.N loop.
func TestCompressionRatios(t *testing.T) {
	if testing.Short() {
		t.Skip("ratio table only printed on full test runs")
	}
	zw, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		t.Fatalf("zstd writer: %v", err)
	}
	defer zw.Close()

	t.Logf("%-22s %10s %10s %10s %10s %10s",
		"payload", "raw", "zstd", "zstd%", "s2", "s2%")
	for name, raw := range samplePayloads() {
		zout := zw.EncodeAll(raw, nil)
		sout := s2.Encode(nil, raw)
		t.Logf("%-22s %10d %10d %9.1f%% %10d %9.1f%%",
			name, len(raw),
			len(zout), 100*float64(len(zout))/float64(len(raw)),
			len(sout), 100*float64(len(sout))/float64(len(raw)),
		)
	}
}

func BenchmarkCompressZstd(b *testing.B) {
	zw, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		b.Fatalf("zstd writer: %v", err)
	}
	defer zw.Close()
	for name, raw := range samplePayloads() {
		raw := raw
		b.Run(name, func(b *testing.B) {
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = zw.EncodeAll(raw, nil)
			}
		})
	}
}

func BenchmarkDecompressZstd(b *testing.B) {
	zw, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		b.Fatalf("zstd writer: %v", err)
	}
	defer zw.Close()
	zr, err := zstd.NewReader(nil)
	if err != nil {
		b.Fatalf("zstd reader: %v", err)
	}
	defer zr.Close()
	for name, raw := range samplePayloads() {
		compressed := zw.EncodeAll(raw, nil)
		raw := raw
		b.Run(name, func(b *testing.B) {
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = zr.DecodeAll(compressed, nil)
			}
		})
	}
}

func BenchmarkCompressS2(b *testing.B) {
	for name, raw := range samplePayloads() {
		raw := raw
		b.Run(name, func(b *testing.B) {
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = s2.Encode(nil, raw)
			}
		})
	}
}

func BenchmarkDecompressS2(b *testing.B) {
	for name, raw := range samplePayloads() {
		compressed := s2.Encode(nil, raw)
		raw := raw
		b.Run(name, func(b *testing.B) {
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = s2.Decode(nil, compressed)
			}
		})
	}
}
