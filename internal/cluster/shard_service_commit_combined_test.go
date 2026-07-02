package cluster

// Combined-commit RPC surface (PUT commit tail collapse): the FRESH wire codec
// (carries logicalShardSize, unlike the batch-promote codec which drops it), the
// handler's per-node promote-before-meta ordering, and the 3-class error wire
// protocol (combined-promote-failed: / combined-meta-failed: / anything else =
// indeterminate).

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeDecodePromoteAndMetaPayload(t *testing.T) {
	tests := []struct {
		name  string
		pairs []stagedPromotePair
		blob  []byte
	}{
		{"pairs and blob", []stagedPromotePair{
			{stagingKey: ".segstaging/txn/blob-1", finalKey: "k/segments/blob-1", logicalShardSize: 2621440},
			{stagingKey: ".segstaging/txn/blob-2", finalKey: "k/segments/blob-2", logicalShardSize: 2621440},
		}, []byte("meta-blob-bytes")},
		{"meta only", nil, []byte("meta-blob-bytes")},
		{"pairs only", []stagedPromotePair{
			{stagingKey: ".segstaging/txn/blob-1", finalKey: "k/segments/blob-1", logicalShardSize: -1},
		}, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			enc, err := encodePromoteAndMetaPayload(tc.pairs, tc.blob)
			require.NoError(t, err)
			pairs, blob, err := decodePromoteAndMetaPayload(enc)
			require.NoError(t, err)
			require.Equal(t, len(tc.pairs), len(pairs))
			for i := range tc.pairs {
				require.Equal(t, tc.pairs[i].stagingKey, pairs[i].stagingKey)
				require.Equal(t, tc.pairs[i].finalKey, pairs[i].finalKey)
				require.Equal(t, tc.pairs[i].logicalShardSize, pairs[i].logicalShardSize)
			}
			if tc.blob == nil {
				require.Nil(t, blob) // hasBlob=0 round-trips as nil, not empty slice
			} else {
				require.Equal(t, tc.blob, blob)
			}
		})
	}
}

func TestDecodePromoteAndMetaPayloadRejectsTruncated(t *testing.T) {
	enc, err := encodePromoteAndMetaPayload([]stagedPromotePair{
		{stagingKey: "a", finalKey: "b", logicalShardSize: 1},
	}, []byte("x"))
	require.NoError(t, err)
	for cut := 1; cut < len(enc); cut++ {
		_, _, derr := decodePromoteAndMetaPayload(enc[:cut])
		require.Error(t, derr, "cut=%d must not decode cleanly", cut)
	}
}

// TestDecodePromoteAndMetaPayloadClampsPrealloc guards the codec against a
// preallocation DoS: a tiny frame whose u32 pair count is 0xFFFFFFFF must fail
// to decode (there are not 4B pairs' worth of bytes) WITHOUT attempting a
// multi-GB slice preallocation.
func TestDecodePromoteAndMetaPayloadClampsPrealloc(t *testing.T) {
	frame := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x00} // count=~4B, then one stray byte
	_, _, err := decodePromoteAndMetaPayload(frame)
	require.Error(t, err)
}

func TestDecodeStagedPromotePairsClampsPrealloc(t *testing.T) {
	frame := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x00} // count=~4B, then one stray byte
	_, err := decodeStagedPromotePairs(frame)
	require.Error(t, err)
}

// --- handler-level tests (real ShardService on a temp dir) -------------------

// stageOneShard writes one staged shard (final-key AAD) so a promote pair for
// (stagingKey → finalKey) succeeds. Mirrors segment_staging_test.go's fixture.
func stageOneShard(t *testing.T, svc *ShardService, bucket, stagingKey, finalKey string, data []byte) {
	t.Helper()
	require.NoError(t, svc.local.writeLocalShardStaged(t.Context(), bucket, stagingKey, finalKey, 0, data))
}

func combinedHandlerBlob(t *testing.T, bucket, key string) []byte {
	t.Helper()
	blob, err := encodeQuorumMetaBlob(PutObjectMetaCmd{
		Bucket: bucket, Key: key, ModTime: 100, ETag: "etag-combined",
		NodeIDs: []string{"self"}, ECData: 1,
	})
	require.NoError(t, err)
	return blob
}

func TestHandlePromoteAndQuorumMetaWrite_PromoteThenMetaOK(t *testing.T) {
	svc, _ := newTestShardService(t)
	const bucket, key = "b", "obj"
	const stagingKey = ".segstaging/txn1/blob1"
	const finalKey = "obj/segments/blob1"
	shard := []byte("combined-shard-payload-0123456789")
	stageOneShard(t, svc, bucket, stagingKey, finalKey, shard)
	blob := combinedHandlerBlob(t, bucket, key)

	payload, err := encodePromoteAndMetaPayload([]stagedPromotePair{
		{stagingKey: stagingKey, finalKey: finalKey, logicalShardSize: int64(len(shard))},
	}, blob)
	require.NoError(t, err)

	resp := svc.handlePromoteAndQuorumMetaWrite(&shardRequest{Bucket: bucket, Key: key, Data: payload})
	rpcType, _, err := unmarshalEnvelope(resp)
	require.NoError(t, err)
	require.Equal(t, "OK", rpcType)

	// Shard promoted to the final path...
	got, err := svc.ReadLocalShard(bucket, finalKey, 0)
	require.NoError(t, err)
	require.Equal(t, shard, got)
	// ...and the meta blob persisted.
	raw, err := svc.readQuorumMetaRaw(bucket, key)
	require.NoError(t, err)
	require.Equal(t, blob, raw)
}

func TestHandlePromoteAndQuorumMetaWrite_PromoteFailureNoMeta(t *testing.T) {
	svc, _ := newTestShardService(t)
	const bucket, key = "b", "obj"
	blob := combinedHandlerBlob(t, bucket, key)
	payload, err := encodePromoteAndMetaPayload([]stagedPromotePair{
		{stagingKey: ".segstaging/txn-missing/blob-missing", finalKey: "obj/segments/blob-missing", logicalShardSize: -1},
	}, blob)
	require.NoError(t, err)

	resp := svc.handlePromoteAndQuorumMetaWrite(&shardRequest{Bucket: bucket, Key: key, Data: payload})
	rpcType, body, err := unmarshalEnvelope(resp)
	require.NoError(t, err)
	require.Equal(t, "Error", rpcType)
	require.True(t, len(body) > len(combinedPromoteFailedPrefix) && string(body[:len(combinedPromoteFailedPrefix)]) == combinedPromoteFailedPrefix,
		"promote failure must carry the promote-class prefix, got %q", body)

	// Per-node ordering: meta must NOT have been written.
	_, err = svc.readQuorumMetaRaw(bucket, key)
	require.Error(t, err)
}

func TestHandlePromoteAndQuorumMetaWrite_MetaOnly(t *testing.T) {
	svc, _ := newTestShardService(t)
	const bucket, key = "b", "obj-meta-only"
	blob := combinedHandlerBlob(t, bucket, key)
	payload, err := encodePromoteAndMetaPayload(nil, blob)
	require.NoError(t, err)

	resp := svc.handlePromoteAndQuorumMetaWrite(&shardRequest{Bucket: bucket, Key: key, Data: payload})
	rpcType, _, err := unmarshalEnvelope(resp)
	require.NoError(t, err)
	require.Equal(t, "OK", rpcType)
	raw, err := svc.readQuorumMetaRaw(bucket, key)
	require.NoError(t, err)
	require.Equal(t, blob, raw)
}

func TestHandlePromoteAndQuorumMetaWrite_MetaFailureMetaClass(t *testing.T) {
	svc, dir := newTestShardService(t)
	const bucket, key = "b", "obj-dir-collision"
	// Force the local meta write to fail: pre-create the target as a non-empty
	// DIRECTORY so the atomic rename over it fails.
	target := filepath.Join(dir, "shards", quorumMetaSubDir, bucket, key)
	require.NoError(t, os.MkdirAll(target, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(target, "occupant"), []byte("x"), 0o644))

	blob := combinedHandlerBlob(t, bucket, key)
	payload, err := encodePromoteAndMetaPayload(nil, blob)
	require.NoError(t, err)

	resp := svc.handlePromoteAndQuorumMetaWrite(&shardRequest{Bucket: bucket, Key: key, Data: payload})
	rpcType, body, err := unmarshalEnvelope(resp)
	require.NoError(t, err)
	require.Equal(t, "Error", rpcType)
	require.True(t, len(body) > len(combinedMetaFailedPrefix) && string(body[:len(combinedMetaFailedPrefix)]) == combinedMetaFailedPrefix,
		"meta failure must carry the meta-class prefix, got %q", body)
}

func TestHandlePromoteAndQuorumMetaWrite_DecodeErrorPromoteClass(t *testing.T) {
	svc, _ := newTestShardService(t)
	payload, err := encodePromoteAndMetaPayload([]stagedPromotePair{
		{stagingKey: "a", finalKey: "b", logicalShardSize: 1},
	}, []byte("x"))
	require.NoError(t, err)

	resp := svc.handlePromoteAndQuorumMetaWrite(&shardRequest{Bucket: "b", Key: "k", Data: payload[:3]})
	rpcType, body, err := unmarshalEnvelope(resp)
	require.NoError(t, err)
	require.Equal(t, "Error", rpcType)
	require.True(t, len(body) > len(combinedPromoteFailedPrefix) && string(body[:len(combinedPromoteFailedPrefix)]) == combinedPromoteFailedPrefix,
		"decode failure (nothing executed) must carry the promote-class prefix, got %q", body)
}

// TestClassifyCombinedCommitError unit-tests the client's 3-class Error-body
// mapping without a live transport (FIX 7a): the promote/meta prefixes map to
// their sentinels; anything else is INDETERMINATE (neither sentinel).
func TestClassifyCombinedCommitError(t *testing.T) {
	promote := classifyCombinedCommitError(combinedPromoteFailedPrefix + "stage gone")
	require.ErrorIs(t, promote, errCombinedPromoteFailed)
	require.NotErrorIs(t, promote, errCombinedMetaFailed)

	meta := classifyCombinedCommitError(combinedMetaFailedPrefix + "disk full")
	require.ErrorIs(t, meta, errCombinedMetaFailed)
	require.NotErrorIs(t, meta, errCombinedPromoteFailed)

	// An old binary's "unknown shard RPC" (unprefixed) is indeterminate: it must
	// carry NEITHER sentinel so the coordinator treats it as abort-class.
	indeterminate := classifyCombinedCommitError("unknown shard RPC: PromoteAndWriteQuorumMeta")
	require.Error(t, indeterminate)
	require.NotErrorIs(t, indeterminate, errCombinedPromoteFailed)
	require.NotErrorIs(t, indeterminate, errCombinedMetaFailed)
}

// TestCombinedCommitErrorBodyRoundTrip drives the REAL handler to produce each
// error class's wire body, then feeds it through the same classification the
// client uses — the end-to-end proof that handler prefix + client mapping agree.
func TestCombinedCommitErrorBodyRoundTrip(t *testing.T) {
	svc, dir := newTestShardService(t)

	// promote class: a missing staged shard fails the promote step.
	promoteBlob := combinedHandlerBlob(t, "b", "k-promote")
	promotePayload, err := encodePromoteAndMetaPayload([]stagedPromotePair{
		{stagingKey: ".segstaging/none/x", finalKey: "k-promote/segments/x", logicalShardSize: -1},
	}, promoteBlob)
	require.NoError(t, err)
	_, body, err := unmarshalEnvelope(svc.handlePromoteAndQuorumMetaWrite(&shardRequest{Bucket: "b", Key: "k-promote", Data: promotePayload}))
	require.NoError(t, err)
	require.ErrorIs(t, classifyCombinedCommitError(string(body)), errCombinedPromoteFailed)

	// meta class: a directory occupying the target path fails the rename.
	target := filepath.Join(dir, "shards", quorumMetaSubDir, "b", "k-meta")
	require.NoError(t, os.MkdirAll(target, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(target, "occupant"), []byte("x"), 0o644))
	metaPayload, err := encodePromoteAndMetaPayload(nil, combinedHandlerBlob(t, "b", "k-meta"))
	require.NoError(t, err)
	_, body, err = unmarshalEnvelope(svc.handlePromoteAndQuorumMetaWrite(&shardRequest{Bucket: "b", Key: "k-meta", Data: metaPayload}))
	require.NoError(t, err)
	require.ErrorIs(t, classifyCombinedCommitError(string(body)), errCombinedMetaFailed)

	// indeterminate: an unprefixed error body (here a truncated rollback payload)
	// classifies to neither sentinel.
	_, body, err = unmarshalEnvelope(svc.handleQuorumMetaRollbackIfMatch(&shardRequest{Bucket: "b", Key: "k3", Data: []byte{0x00}}))
	require.NoError(t, err)
	cls := classifyCombinedCommitError(string(body))
	require.Error(t, cls)
	require.NotErrorIs(t, cls, errCombinedPromoteFailed)
	require.NotErrorIs(t, cls, errCombinedMetaFailed)
}

// TestHandlePromoteAndQuorumMetaWrite_OverwriteRestoresPrevious drives the
// handler-level restore-previous path (FIX 3): write blob A, combined-write blob
// B (the OK body carries prev=A), then roll back with (B, A, true) → the file
// content is back to A.
func TestHandlePromoteAndQuorumMetaWrite_OverwriteRestoresPrevious(t *testing.T) {
	svc, _ := newTestShardService(t)
	const bucket, key = "b", "obj-overwrite"

	blobA, err := encodeQuorumMetaBlob(PutObjectMetaCmd{
		Bucket: bucket, Key: key, ModTime: 100, ETag: "A", NodeIDs: []string{"self"}, ECData: 1,
	})
	require.NoError(t, err)
	blobB, err := encodeQuorumMetaBlob(PutObjectMetaCmd{
		Bucket: bucket, Key: key, ModTime: 200, ETag: "B", NodeIDs: []string{"self"}, ECData: 1,
	})
	require.NoError(t, err)

	// Write A.
	require.NoError(t, svc.writeQuorumMetaLocal(bucket, key, blobA))

	// Combined-write B; the OK body carries the overwritten previous (A).
	payload, err := encodePromoteAndMetaPayload(nil, blobB)
	require.NoError(t, err)
	rpcType, okBody, err := unmarshalEnvelope(svc.handlePromoteAndQuorumMetaWrite(&shardRequest{Bucket: bucket, Key: key, Data: payload}))
	require.NoError(t, err)
	require.Equal(t, "OK", rpcType)
	previous, hadPrevious := decodeCombinedOKBody(okBody)
	require.True(t, hadPrevious, "overwrite must report a previous blob")
	require.Equal(t, blobA, previous, "the OK body must carry the exact overwritten blob (A)")

	raw, err := svc.readQuorumMetaRaw(bucket, key)
	require.NoError(t, err)
	require.Equal(t, blobB, raw, "B is now the latest")

	// Abort rollback with (expected=B, previous=A, hadPrevious=true) restores A.
	rpcType, _, err = unmarshalEnvelope(svc.handleQuorumMetaRollbackIfMatch(&shardRequest{
		Bucket: bucket, Key: key, Data: encodeRollbackPayload(blobB, previous, hadPrevious),
	}))
	require.NoError(t, err)
	require.Equal(t, "OK", rpcType)
	raw, err = svc.readQuorumMetaRaw(bucket, key)
	require.NoError(t, err)
	require.Equal(t, blobA, raw, "overwrite abort must RESTORE the previous blob (A), not delete it")
}

func TestHandleQuorumMetaRollbackIfMatch(t *testing.T) {
	svc, _ := newTestShardService(t)
	const bucket, key = "b", "obj-rollback"
	blob := combinedHandlerBlob(t, bucket, key)

	// Write via the plain meta-write handler, then roll back with the SAME blob.
	resp := svc.handleQuorumMetaWrite(&shardRequest{Bucket: bucket, Key: key, Data: blob})
	rpcType, _, err := unmarshalEnvelope(resp)
	require.NoError(t, err)
	require.Equal(t, "OK", rpcType)

	resp = svc.handleQuorumMetaRollbackIfMatch(&shardRequest{Bucket: bucket, Key: key, Data: encodeRollbackPayload(blob, nil, false)})
	rpcType, _, err = unmarshalEnvelope(resp)
	require.NoError(t, err)
	require.Equal(t, "OK", rpcType)
	_, err = svc.readQuorumMetaRaw(bucket, key)
	require.Error(t, err, "content-matched rollback must delete the blob")

	// Write again; a rollback with DIFFERENT content is a no-op success.
	resp = svc.handleQuorumMetaWrite(&shardRequest{Bucket: bucket, Key: key, Data: blob})
	rpcType, _, err = unmarshalEnvelope(resp)
	require.NoError(t, err)
	require.Equal(t, "OK", rpcType)
	other, err := encodeQuorumMetaBlob(PutObjectMetaCmd{
		Bucket: bucket, Key: key, ModTime: 200, ETag: fmt.Sprintf("etag-%d", 2),
		NodeIDs: []string{"self"}, ECData: 1,
	})
	require.NoError(t, err)
	resp = svc.handleQuorumMetaRollbackIfMatch(&shardRequest{Bucket: bucket, Key: key, Data: encodeRollbackPayload(other, nil, false)})
	rpcType, _, err = unmarshalEnvelope(resp)
	require.NoError(t, err)
	require.Equal(t, "OK", rpcType, "content-mismatch rollback is a no-op success")
	raw, err := svc.readQuorumMetaRaw(bucket, key)
	require.NoError(t, err)
	require.Equal(t, blob, raw, "mismatched rollback must leave the blob untouched")
}
