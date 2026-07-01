package cluster

import (
	"bytes"
	"context"
	"crypto/md5"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestRelocationStillEligible(t *testing.T) {
	const (
		etag = "etag-abc"
		ver  = "ver-123"
	)
	base := relocateInput{Bucket: "b", Key: "k", VersionID: ver, ExpectedETag: etag}

	tests := []struct {
		name             string
		cur              PutObjectMetaCmd
		in               relocateInput
		clusterRedundant bool
		wantErr          bool
	}{
		{
			name:             "eligible non-redundant matching etag/version redundant cluster",
			cur:              PutObjectMetaCmd{ECData: 1, ECParity: 0, ETag: etag, VersionID: ver},
			in:               base,
			clusterRedundant: true,
			wantErr:          false,
		},
		{
			name:             "already redundant parity>0",
			cur:              PutObjectMetaCmd{ECData: 4, ECParity: 2, ETag: etag, VersionID: ver},
			in:               base,
			clusterRedundant: true,
			wantErr:          true,
		},
		{
			name:             "etag mismatch",
			cur:              PutObjectMetaCmd{ECData: 1, ECParity: 0, ETag: "stale", VersionID: ver},
			in:               base,
			clusterRedundant: true,
			wantErr:          true,
		},
		{
			name:             "version mismatch",
			cur:              PutObjectMetaCmd{ECData: 1, ECParity: 0, ETag: etag, VersionID: "other"},
			in:               base,
			clusterRedundant: true,
			wantErr:          true,
		},
		{
			name:             "delete marker",
			cur:              PutObjectMetaCmd{ECData: 1, ECParity: 0, ETag: etag, VersionID: ver, IsDeleteMarker: true},
			in:               base,
			clusterRedundant: true,
			wantErr:          true,
		},
		{
			name:             "cluster not redundant",
			cur:              PutObjectMetaCmd{ECData: 1, ECParity: 0, ETag: etag, VersionID: ver},
			in:               base,
			clusterRedundant: false,
			wantErr:          true,
		},
		{
			name:             "data shards zero",
			cur:              PutObjectMetaCmd{ECData: 0, ECParity: 0, ETag: etag, VersionID: ver},
			in:               base,
			clusterRedundant: true,
			wantErr:          true,
		},
		{
			name:             "appendable object with embedded digest history is relocatable",
			cur:              PutObjectMetaCmd{ECData: 1, ECParity: 0, ETag: etag, VersionID: ver, IsAppendable: true, AppendCallMD5s: [][]byte{[]byte("aaaaaaaaaaaaaaaa")}},
			in:               base,
			clusterRedundant: true,
			wantErr:          false,
		},
		{
			name:             "coalesced appendable object with embedded digest history is relocatable",
			cur:              PutObjectMetaCmd{ECData: 1, ECParity: 0, ETag: etag, VersionID: ver, IsAppendable: true, Coalesced: []CoalescedShardRef{{CoalescedID: "x"}}, AppendCallMD5s: [][]byte{[]byte("aaaaaaaaaaaaaaaa")}},
			in:               base,
			clusterRedundant: true,
			wantErr:          false,
		},
		{
			name:             "side-record-only appendable object is relocatable",
			cur:              PutObjectMetaCmd{ECData: 1, ECParity: 0, ETag: etag, VersionID: ver, IsAppendable: true},
			in:               base,
			clusterRedundant: true,
			wantErr:          false,
		},
		{
			name:             "coalesced appendable object without embedded digest history is relocatable",
			cur:              PutObjectMetaCmd{ECData: 1, ECParity: 0, ETag: etag, VersionID: ver, IsAppendable: true, Coalesced: []CoalescedShardRef{{CoalescedID: "x"}}},
			in:               base,
			clusterRedundant: true,
			wantErr:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := relocationStillEligible(tt.cur, tt.in, tt.clusterRedundant)
			if tt.wantErr {
				require.ErrorIs(t, err, ErrRelocateSkipped)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestRelocatedMetaPreservesAppendIdentity(t *testing.T) {
	cur := PutObjectMetaCmd{
		IsAppendable:   true,
		AppendCallMD5s: [][]byte{[]byte("aaaaaaaaaaaaaaaa"), []byte("bbbbbbbbbbbbbbbb")},
		ETag:           "composite-etag-2",
		MetaSeq:        7,
	}
	cmd := PutObjectMetaCmd{
		ExpectedETag: "old-etag",
		ETag:         "whole-body-md5",
		Segments:     []SegmentMetaEntry{{BlobID: "new-seg", Size: 32}},
		ECData:       4,
		ECParity:     2,
	}

	got := relocatedMetaCmd(cur, cmd)

	require.True(t, got.IsAppendable)
	require.Equal(t, cur.ETag, got.ETag)
	require.Equal(t, cur.AppendCallMD5s, got.AppendCallMD5s)
	require.Empty(t, got.ExpectedETag)
	require.Equal(t, uint64(8), got.MetaSeq)

	got.AppendCallMD5s[0][0] = 'z'
	require.Equal(t, byte('a'), cur.AppendCallMD5s[0][0], "relocated metadata must clone digest history")
}

func TestRelocateAppendableEmbeddedHistoryPreservesFutureAppendETag(t *testing.T) {
	ctx := context.Background()
	b := newSingleNode1Plus0ChunkCapable(t)
	b.appendSideRecordsDisabled = true
	require.NoError(t, b.CreateBucket(ctx, "bk"))

	_, err := b.AppendObject(ctx, "bk", "k", 0, bytes.NewReader([]byte("aaa")))
	require.NoError(t, err)
	obj, err := b.AppendObject(ctx, "bk", "k", 3, bytes.NewReader([]byte("bbbb")))
	require.NoError(t, err)
	require.Equal(t, int64(7), obj.Size)

	before, err := b.readQuorumMetaCmd("bk", "k")
	require.NoError(t, err)
	require.True(t, before.IsAppendable)
	require.Equal(t, uint8(0), before.ECParity)
	require.Len(t, before.AppendCallMD5s, 2)
	require.NotEmpty(t, before.Segments)

	configureRelocationRedundantCapacity(t, b)
	err = b.relocateObjectToRedundantGroup(ctx, relocateInput{
		Bucket:       "bk",
		Key:          "k",
		VersionID:    before.VersionID,
		ExpectedETag: before.ETag,
	})
	require.NoError(t, err)

	relocated, err := b.readQuorumMetaCmd("bk", "k")
	require.NoError(t, err)
	require.True(t, relocated.IsAppendable)
	require.Equal(t, uint8(1), relocated.ECParity)
	require.Equal(t, before.ETag, relocated.ETag)
	require.Equal(t, before.VersionID, relocated.VersionID)
	require.Equal(t, before.AppendCallMD5s, relocated.AppendCallMD5s)
	require.NotEmpty(t, relocated.Segments)
	require.Empty(t, relocated.Coalesced)

	rc, gotObj, err := b.GetObject(ctx, "bk", "k")
	require.NoError(t, err)
	got, readErr := io.ReadAll(rc)
	closeErr := rc.Close()
	require.NoError(t, readErr)
	require.NoError(t, closeErr)
	require.Equal(t, []byte("aaabbbb"), got)
	require.True(t, gotObj.IsAppendable)

	appended, err := b.AppendObject(ctx, "bk", "k", 7, bytes.NewReader([]byte("cc")))
	require.NoError(t, err)
	wantETag := storage.CompositeETag([][]byte{
		before.AppendCallMD5s[0],
		before.AppendCallMD5s[1],
		md5Bytes([]byte("cc")),
	})
	require.Equal(t, wantETag, appended.ETag)
}

func TestRelocateAppendSideRecordObjectPreservesFutureAppendETag(t *testing.T) {
	ctx := context.Background()
	b := newSingleNode1Plus0ChunkCapable(t)
	require.NoError(t, b.CreateBucket(ctx, "bk"))

	_, err := b.AppendObject(ctx, "bk", "k", 0, bytes.NewReader([]byte("aaa")))
	require.NoError(t, err)
	obj, err := b.AppendObject(ctx, "bk", "k", 3, bytes.NewReader([]byte("bbbb")))
	require.NoError(t, err)
	require.Equal(t, int64(7), obj.Size)

	before, err := b.readQuorumMetaCmd("bk", "k")
	require.NoError(t, err)
	require.True(t, before.IsAppendable)
	require.Equal(t, uint8(0), before.ECParity)
	require.Empty(t, before.AppendCallMD5s)
	require.Empty(t, before.Segments)
	beforeSummary, err := b.readClusterAppendSummary(ctx, "bk", "k", before.VersionID, before.NodeIDs)
	require.NoError(t, err)

	configureRelocationRedundantCapacity(t, b)
	err = b.relocateObjectToRedundantGroup(ctx, relocateInput{
		Bucket:       "bk",
		Key:          "k",
		VersionID:    before.VersionID,
		ExpectedETag: before.ETag,
	})
	require.NoError(t, err)

	// The side-record relocate path re-encodes the body directly into the EC
	// stream encoder via an inline md5 tee — it no longer spools the GetObject
	// read-back stream to a disk temp file, so the EC source-spool dir must never
	// be created during relocate.
	_, statErr := os.Stat(filepath.Join(b.root, "tmp", "ec-spool"))
	require.True(t, os.IsNotExist(statErr), "ec source-spool dir must not be created during relocate")

	relocated, err := b.readQuorumMetaCmd("bk", "k")
	require.NoError(t, err)
	require.True(t, relocated.IsAppendable)
	require.Equal(t, uint8(1), relocated.ECParity)
	require.Equal(t, before.ETag, relocated.ETag)
	require.Empty(t, relocated.Segments)
	require.Len(t, relocated.Coalesced, 1)
	relocatedSummary, err := b.readClusterAppendSummary(ctx, "bk", "k", relocated.VersionID, relocated.NodeIDs)
	require.NoError(t, err)
	require.Zero(t, relocatedSummary.Size)
	require.Zero(t, relocatedSummary.SegmentCount)
	require.Equal(t, beforeSummary.ETagDigestState, relocatedSummary.ETagDigestState)
	require.Equal(t, beforeSummary.ETagPartCount, relocatedSummary.ETagPartCount)

	rc, _, err := b.GetObject(ctx, "bk", "k")
	require.NoError(t, err)
	got, readErr := io.ReadAll(rc)
	closeErr := rc.Close()
	require.NoError(t, readErr)
	require.NoError(t, closeErr)
	require.Equal(t, []byte("aaabbbb"), got)

	appended, err := b.AppendObject(ctx, "bk", "k", 7, bytes.NewReader([]byte("cc")))
	require.NoError(t, err)
	state, count, err := storage.AppendETagStateAppend(beforeSummary.ETagDigestState, beforeSummary.ETagPartCount, md5Bytes([]byte("cc")))
	require.NoError(t, err)
	wantETag, err := storage.CompositeETagFromState(state, count)
	require.NoError(t, err)
	require.Equal(t, wantETag, appended.ETag)
}

func configureRelocationRedundantCapacity(t *testing.T, b *DistributedBackend) {
	t.Helper()
	self := b.currentSelfAddr()
	b.SetECConfig(ECConfig{DataShards: 2, ParityShards: 1})
	b.SetShardGroupSource(&fakeGenShardSource{
		groups: map[string]ShardGroupEntry{
			"group-0": {ID: "group-0", PeerIDs: []string{self, self, self}},
		},
		nodeCount: 3,
	})
}

func md5Bytes(data []byte) []byte {
	sum := md5.Sum(data)
	return sum[:]
}
