package receipt

import (
	"errors"
	"fmt"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/receipt/receiptpb"
)

var ErrLegacyStorageFormat = errors.New("receipt: legacy storage format detected (wipe-and-restart required)")

// EncodeReceipt and DecodeReceiptStorage are exported because
// internal/cluster/receipt_broadcast.go consumes them. Other storage codecs
// (packblob, eventstore) stay package-private (lowercase).
func EncodeReceipt(r *HealReceipt) ([]byte, error) {
	b := flatbuffers.NewBuilder(512)

	ridOff := b.CreateString(r.ReceiptID)
	kidOff := b.CreateString(r.KeyID)
	bucketOff := b.CreateString(r.Object.Bucket)
	keyOff := b.CreateString(r.Object.Key)
	verOff := b.CreateString(r.Object.VersionID)
	receiptpb.ObjectRefStart(b)
	receiptpb.ObjectRefAddBucket(b, bucketOff)
	receiptpb.ObjectRefAddKey(b, keyOff)
	receiptpb.ObjectRefAddVersionId(b, verOff)
	objOff := receiptpb.ObjectRefEnd(b)

	shardsLostVec := buildInt32Vec(b, r.ShardsLost)
	shardsRebuiltVec := buildInt32Vec(b, r.ShardsRebuilt)
	peersVec := buildStringVec(b, r.PeersInvolved)
	eventIDsVec := buildStringVec(b, r.EventIDs)

	cidOff := b.CreateString(r.CorrelationID)
	cpOff := b.CreateString(r.CanonicalPayload)
	sigOff := b.CreateString(r.Signature)

	receiptpb.HealReceiptStart(b)
	receiptpb.HealReceiptAddReceiptId(b, ridOff)
	receiptpb.HealReceiptAddKeyId(b, kidOff)
	receiptpb.HealReceiptAddTimestampUnixNs(b, r.Timestamp.UnixNano())
	receiptpb.HealReceiptAddObject(b, objOff)
	receiptpb.HealReceiptAddShardsLost(b, shardsLostVec)
	receiptpb.HealReceiptAddShardsRebuilt(b, shardsRebuiltVec)
	receiptpb.HealReceiptAddParityUsed(b, int32(r.ParityUsed))
	receiptpb.HealReceiptAddPeersInvolved(b, peersVec)
	receiptpb.HealReceiptAddDurationMs(b, r.DurationMs)
	receiptpb.HealReceiptAddEventIds(b, eventIDsVec)
	receiptpb.HealReceiptAddCorrelationId(b, cidOff)
	receiptpb.HealReceiptAddCanonicalPayload(b, cpOff)
	receiptpb.HealReceiptAddSignature(b, sigOff)
	b.Finish(receiptpb.HealReceiptEnd(b))
	return b.FinishedBytes(), nil
}

func DecodeReceiptStorage(data []byte) (out *HealReceipt, err error) {
	// Whitespace-tolerant legacy guard: skip leading space/tab/LF/CR then
	// detect JSON by '{'. Matches Task 1/2 pattern (packblob/apply-quarantine).
	trimmed := data
	for len(trimmed) > 0 && (trimmed[0] == ' ' || trimmed[0] == '\t' || trimmed[0] == '\n' || trimmed[0] == '\r') {
		trimmed = trimmed[1:]
	}
	if len(trimmed) > 0 && trimmed[0] == '{' {
		return nil, ErrLegacyStorageFormat
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("decode receipt storage: malformed FB: %v", r)
		}
	}()
	t := receiptpb.GetRootAsHealReceipt(data, 0)
	obj := t.Object(nil)
	r := &HealReceipt{
		ReceiptID:        string(t.ReceiptId()),
		KeyID:            string(t.KeyId()),
		Timestamp:        time.Unix(0, t.TimestampUnixNs()).UTC(),
		Object:           ObjectRef{Bucket: string(obj.Bucket()), Key: string(obj.Key()), VersionID: string(obj.VersionId())},
		ShardsLost:       readInt32Vec(t.ShardsLostLength(), t.ShardsLost),
		ShardsRebuilt:    readInt32Vec(t.ShardsRebuiltLength(), t.ShardsRebuilt),
		ParityUsed:       int(t.ParityUsed()),
		PeersInvolved:    readStringVec(t.PeersInvolvedLength(), t.PeersInvolved),
		DurationMs:       t.DurationMs(),
		EventIDs:         readStringVec(t.EventIdsLength(), t.EventIds),
		CorrelationID:    string(t.CorrelationId()),
		CanonicalPayload: string(t.CanonicalPayload()),
		Signature:        string(t.Signature()),
	}
	return r, nil
}

// Generic vector builders (not tied to a specific field).
// FB Go generator emits per-field StartXxxVector helpers, but they all
// call b.StartVector(elemSize, n, alignment) internally — so the generic
// form works for any vector and avoids field-name coupling.
func buildInt32Vec(b *flatbuffers.Builder, xs []int32) flatbuffers.UOffsetT {
	b.StartVector(4, len(xs), 4) // int32 = 4 bytes, align 4
	for i := len(xs) - 1; i >= 0; i-- {
		b.PrependInt32(xs[i])
	}
	return b.EndVector(len(xs))
}

func buildStringVec(b *flatbuffers.Builder, xs []string) flatbuffers.UOffsetT {
	offs := make([]flatbuffers.UOffsetT, len(xs))
	for i, s := range xs {
		offs[i] = b.CreateString(s)
	}
	b.StartVector(4, len(offs), 4) // UOffsetT = uint32 = 4 bytes
	for i := len(offs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offs[i])
	}
	return b.EndVector(len(offs))
}

func readInt32Vec(n int, getter func(int) int32) []int32 {
	if n == 0 {
		return nil
	}
	out := make([]int32, n)
	for i := 0; i < n; i++ {
		out[i] = getter(i)
	}
	return out
}

func readStringVec(n int, getter func(int) []byte) []string {
	if n == 0 {
		return nil
	}
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = string(getter(i))
	}
	return out
}
