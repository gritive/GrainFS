package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// md5("hello") = 5d41402abc4b2a76b9719d911017c592
const helloMD5Hex = "5d41402abc4b2a76b9719d911017c592"

// md5("") = d41d8cd98f00b204e9800998ecf8427e
const emptyMD5Hex = "d41d8cd98f00b204e9800998ecf8427e"

// readerOnly hides bytes.Reader's ReaderAt/Len/Size so a PUT carries no
// SizeHint and therefore takes the spool write path (the streaming path is gated
// on SizeHint + SizeHintExact).
type readerOnly struct{ r io.Reader }

func (ro readerOnly) Read(p []byte) (int, error) { return ro.r.Read(p) }

// newStreamingBackend builds an EC backend with a real shard group wired
// (shardGroup != nil), so a sized PUT (SizeHint + SizeHintExact) takes the
// no-spool streaming path — the regime this file exercises. newTestDistributedBackend
// leaves shardGroup nil, which forces the spool path (used by the spool tests below).
func newStreamingBackend(t *testing.T) *DistributedBackend {
	t.Helper()
	b := setupECBackend(t)
	b.SetShardGroupSource(&fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-a": {ID: "group-a", PeerIDs: []string{"self", "self", "self"}},
	}})
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))
	return b
}

// requireNoSpoolDir proves no spool was used by the PUT: spoolObject's first
// action is MkdirAll(<root>/tmp/put-spool), and the dir is not pre-created at
// backend init, so its absence means spoolObject was never called. The streaming
// path never spools; the spool path always creates this dir (it persists even
// after the temp file is cleaned up). A read-only freeze would not work here —
// spoolObject chmods the dir back to 0o700 — so dir-absence is the discriminator.
func requireNoSpoolDir(t *testing.T, b *DistributedBackend) {
	t.Helper()
	_, err := os.Stat(b.spoolDir())
	require.True(t, errors.Is(err, fs.ErrNotExist),
		"spool dir must not exist (streaming path must not spool); stat err: %v", err)
}

// finalShardFileCount walks every shard data dir and counts shard_* files under
// the bucket that are NOT in the .segstaging orphan namespace — i.e. promoted /
// committed FINAL shards (key/segments/<blobID>/shard_N). A non-zero count means
// the object was (partly) promoted/committed; zero means the commit was aborted
// before any promote.
func finalShardFileCount(t *testing.T, b *DistributedBackend, bucket string) int {
	t.Helper()
	stagingSeg := string(filepath.Separator) + SegStagingPrefix + string(filepath.Separator)
	n := 0
	for _, dir := range b.shardSvc.DataDirs() {
		_ = filepath.WalkDir(filepath.Join(dir, bucket), func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil // bucket dir may not exist yet
			}
			if !d.IsDir() && strings.HasPrefix(d.Name(), "shard_") && !strings.Contains(p, stagingSeg) {
				n++
			}
			return nil
		})
	}
	return n
}

// TestPutObject_BareBytesReader_Streams: the bare PutObject convenience (no
// SizeHint) with a *bytes.Reader body now takes the no-spool streaming path —
// PutObjectWithRequest stamps the reader's exact Len() as SizeHintExact centrally.
// This is the discriminator that proves internal sized-reader callers (WriteAt /
// Truncate RMW, forwarded small bodies) no longer spool. requireNoSpoolDir proves
// spoolObject (MkdirAll <root>/tmp/put-spool) was never reached; the round-trip
// proves the body is stored intact.
func TestPutObject_BareBytesReader_Streams(t *testing.T) {
	b := newStreamingBackend(t)
	ctx := context.Background()

	data := []byte("bare bytes.Reader must stream, not spool")
	obj, err := b.PutObject(ctx, "bucket", "bare", bytes.NewReader(data), "application/octet-stream")
	require.NoError(t, err)
	require.Equal(t, int64(len(data)), obj.Size)
	requireNoSpoolDir(t, b)

	rc, _, err := b.GetObject(ctx, "bucket", "bare")
	require.NoError(t, err)
	got, readErr := io.ReadAll(rc)
	require.NoError(t, rc.Close())
	require.NoError(t, readErr)
	require.Equal(t, data, got)
}

// TestPutObject_BareReaderOnly_Spools: a body that hides Len() (readerOnly) and
// carries no SizeHint still falls through to the spool path even on a streaming
// backend — the central Len() detection cannot size it. This pins the boundary of
// the no-spool optimization (genuinely-unknown-size streams remain on spool).
func TestPutObject_BareReaderOnly_Spools(t *testing.T) {
	b := newStreamingBackend(t)
	ctx := context.Background()

	data := []byte("no Len() so this spools")
	obj, err := b.PutObject(ctx, "bucket", "unsized", readerOnly{r: bytes.NewReader(data)}, "application/octet-stream")
	require.NoError(t, err)
	require.Equal(t, int64(len(data)), obj.Size)
	_, statErr := os.Stat(b.spoolDir())
	require.NoError(t, statErr, "unsized body must have spooled (spool dir created)")
}

// TestPutObject_SizedContentMD5_Mismatch_NoSpool: a sized PUT (SizeHint +
// SizeHintExact) with a WRONG Content-MD5 is rejected as BadDigest via the
// streaming tee-validate before-commit hook — without spooling. requireNoSpoolDir
// proves no spool was used (spoolObject would have created <root>/tmp/put-spool).
func TestPutObject_SizedContentMD5_Mismatch_NoSpool(t *testing.T) {
	b := newStreamingBackend(t)
	ctx := context.Background()

	sz := int64(len("hello"))
	_, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket:        "bucket",
		Key:           "sized",
		Body:          bytes.NewReader([]byte("hello")),
		SizeHint:      &sz,
		SizeHintExact: true,
		ContentMD5Hex: "deadbeefdeadbeefdeadbeefdeadbeef", // wrong
	})
	require.ErrorIs(t, err, storage.ErrContentMD5Mismatch)
	requireNoSpoolDir(t, b)

	// Aborted before commit ⇒ object never visible.
	_, _, gerr := b.GetObject(ctx, "bucket", "sized")
	require.ErrorIs(t, gerr, storage.ErrObjectNotFound)
}

// TestPutObject_SizedContentMD5_Match_Streams: a sized PUT with a CORRECT
// Content-MD5 succeeds via the streaming path (spool dir never created) and the
// stored ETag equals the body md5.
func TestPutObject_SizedContentMD5_Match_Streams(t *testing.T) {
	b := newStreamingBackend(t)
	ctx := context.Background()

	sz := int64(len("hello"))
	obj, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket:        "bucket",
		Key:           "sized",
		Body:          bytes.NewReader([]byte("hello")),
		SizeHint:      &sz,
		SizeHintExact: true,
		ContentMD5Hex: helloMD5Hex,
	})
	require.NoError(t, err)
	require.Equal(t, helloMD5Hex, obj.ETag)
	requireNoSpoolDir(t, b)
}

// TestPutObject_SizedContentMD5_Mismatch_NoLeak: a streaming-path mismatch aborts
// the commit at the beforeCommit hook — a previously-unexercised path (user PUTs
// always passed beforeCommit=nil). The object must never be visible and no FINAL
// (promoted/committed) shards may remain.
//
// Staging note: the chunked path writes shards to .segstaging/<txn>/... first and
// promotes them only at commit. On the aborted commit the cleanup defer issues a
// self-RPC DeleteShards to reclaim the staged shards — but this unit harness wires
// a nil shard transport (NewShardService(dir, nil)), so the self-delete RPC cannot
// loop back in-process (production self-deletes loop through the real transport).
// Asserting .segstaging emptiness is therefore not possible here; instead we assert
// the committed state: GET is not-found (never visible) and zero FINAL shards
// (nothing was promoted). Any .segstaging residue is in the orphan namespace the
// walker age-outs as a backstop.
func TestPutObject_SizedContentMD5_Mismatch_NoLeak(t *testing.T) {
	b := newStreamingBackend(t)
	ctx := context.Background()

	sz := int64(len("hello"))
	_, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket:        "bucket",
		Key:           "leak",
		Body:          bytes.NewReader([]byte("hello")),
		SizeHint:      &sz,
		SizeHintExact: true,
		ContentMD5Hex: "deadbeefdeadbeefdeadbeefdeadbeef", // wrong
	})
	require.ErrorIs(t, err, storage.ErrContentMD5Mismatch)
	requireNoSpoolDir(t, b)

	_, _, gerr := b.GetObject(ctx, "bucket", "leak")
	require.ErrorIs(t, gerr, storage.ErrObjectNotFound, "aborted commit ⇒ object never visible")
	require.Zero(t, finalShardFileCount(t, b, "bucket"), "aborted commit must leave no final (promoted) shards")
}

// TestRunChunkedPut_BeforeCommitError_ReclaimsStagedShards is the focused
// "cleanup path ran" companion to NoLeak. It drives runChunkedPut directly (where
// deleteShardsFn is injectable) against a REAL single-node EC backend with staging
// active, then fails the beforeCommit hook. It asserts the error propagates AND the
// cleanup defer requested deletion of the STAGED shard key for every written
// segment blob — proving the beforeCommit-error path reclaims staged shards with
// the correct key, independent of the (nil) shard transport. The spy records calls
// rather than deleting; on-disk reclaim is covered by the real self-RPC loopback in
// production (see NoLeak's note).
func TestRunChunkedPut_BeforeCommitError_ReclaimsStagedShards(t *testing.T) {
	ctx := context.Background()
	b, bucket, key, body := realStagingBackend(t)
	numSegments := int((int64(len(body)) + int64(testChunkedPutChunkSize) - 1) / int64(testChunkedPutChunkSize))
	const txnID = "txn-beforecommit"
	csb := newRealStagingCSB(t, b, bucket, key, txnID, numSegments)

	var deleted []string
	csb.deleteShardsFn = func(_ context.Context, _, _, shardKey string) error {
		deleted = append(deleted, shardKey) // cleanup defer is single-threaded
		return nil
	}

	errBoom := errors.New("boom")
	_, err := runChunkedPut(ctx, csb, bytes.NewReader(body),
		bucket, key, "v1", "application/octet-stream", nil, "", 0, false, "",
		func() error { return errBoom }, nil, nil)
	require.ErrorIs(t, err, errBoom)

	require.Len(t, csb.placements, numSegments)
	for _, p := range csb.placements {
		require.NotEmpty(t, p.BlobID, "real write must record a blob per segment")
		require.Contains(t, deleted, segmentStagingShardKey(txnID, p.BlobID),
			"cleanup defer must request deletion of the staged shard for blob %s", p.BlobID)
	}
}

// TestPutObject_SizedContentMD5_EmptyBody: a 0-byte body with a correct
// Content-MD5 and SizeHint=0 streams successfully and creates an empty object.
func TestPutObject_SizedContentMD5_EmptyBody(t *testing.T) {
	b := newStreamingBackend(t)
	ctx := context.Background()

	sz := int64(0)
	obj, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket:        "bucket",
		Key:           "empty",
		Body:          bytes.NewReader(nil),
		SizeHint:      &sz,
		SizeHintExact: true,
		ContentMD5Hex: emptyMD5Hex,
	})
	require.NoError(t, err)
	require.Equal(t, emptyMD5Hex, obj.ETag)
	require.Equal(t, int64(0), obj.Size)
	requireNoSpoolDir(t, b)
}

// TestPutObject_ContentMD5_ETagParity: storing the same body with and without
// Content-MD5 yields a byte-identical stored ETag (both take the streaming path;
// ETag derives from WriteSized in both). The no-MD5 store's ETag is reused as the
// client digest for the with-MD5 store, proving they agree. The chunk size is
// lowered to testChunkedPutChunkSize so the body spans multiple segments — the tee
// (which sees the full plaintext across all segments) must not perturb the ETag.
func TestPutObject_ContentMD5_ETagParity(t *testing.T) {
	b := newStreamingBackend(t)
	b.chunkedPutChunkSize = testChunkedPutChunkSize // small chunks → multi-segment
	ctx := context.Background()

	body := makeChunkedTestBody(testChunkedPutChunkSize + 4096) // 2 segments
	sz := int64(len(body))
	require.Greater(t, sz, int64(testChunkedPutChunkSize), "body must span >1 segment")

	noMD5, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket:        "bucket",
		Key:           "a",
		Body:          bytes.NewReader(body),
		SizeHint:      &sz,
		SizeHintExact: true,
	})
	require.NoError(t, err)

	withMD5, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket:        "bucket",
		Key:           "b",
		Body:          bytes.NewReader(body),
		SizeHint:      &sz,
		SizeHintExact: true,
		ContentMD5Hex: noMD5.ETag, // == md5(body); must agree and store identically
	})
	require.NoError(t, err)
	require.Equal(t, noMD5.ETag, withMD5.ETag)
}

// TestPutObject_SizedReader_ContentMD5Mismatch: a small PUT whose body is a
// sized reader (bytes.NewReader) but carries NO SizeHint takes the spool path; a
// wrong Content-MD5 is rejected as BadDigest at the spool site (sp.ETag) before
// any shard write.
func TestPutObject_SizedReader_ContentMD5Mismatch(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bucket"))

	_, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket:        "bucket",
		Key:           "fast",
		Body:          bytes.NewReader([]byte("hello")),
		ContentMD5Hex: "deadbeefdeadbeefdeadbeefdeadbeef", // wrong
	})
	require.ErrorIs(t, err, storage.ErrContentMD5Mismatch)
}

// TestPutObject_SpoolPath_ContentMD5Mismatch: a PUT whose body is not a
// sized-reader (forces the spool path) with a wrong Content-MD5 must be rejected
// as BadDigest at the spool site (sp.ETag) before any shard write.
func TestPutObject_SpoolPath_ContentMD5Mismatch(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bucket"))

	_, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket:        "bucket",
		Key:           "spool",
		Body:          readerOnly{r: bytes.NewReader([]byte("hello"))},
		ContentMD5Hex: "deadbeefdeadbeefdeadbeefdeadbeef", // wrong
	})
	require.ErrorIs(t, err, storage.ErrContentMD5Mismatch)
}

// TestPutObject_WithSizeHint_ContentMD5Mismatch: a PUT carrying SizeHint but NOT
// SizeHintExact stays on the spool path (the streaming gate requires
// SizeHintExact), so Content-MD5 is validated at the spool site. A wrong digest is
// rejected as BadDigest.
func TestPutObject_WithSizeHint_ContentMD5Mismatch(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bucket"))

	sz := int64(5)
	_, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket:        "bucket",
		Key:           "sized",
		Body:          bytes.NewReader([]byte("hello")),
		SizeHint:      &sz,
		ContentMD5Hex: "deadbeefdeadbeefdeadbeefdeadbeef", // wrong
	})
	require.ErrorIs(t, err, storage.ErrContentMD5Mismatch)
}

// TestPutObject_ContentMD5Match: a correct Content-MD5 succeeds and the stored
// ETag equals the body md5. Both subtests carry no SizeHint, so both take the
// spool path.
func TestPutObject_ContentMD5Match(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bucket"))

	for _, tc := range []struct {
		name string
		body io.Reader
		key  string
	}{
		{"reader", bytes.NewReader([]byte("hello")), "ok-reader"},
		{"spool", readerOnly{r: bytes.NewReader([]byte("hello"))}, "ok-spool"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			obj, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
				Bucket:        "bucket",
				Key:           tc.key,
				Body:          tc.body,
				ContentMD5Hex: helloMD5Hex,
			})
			require.NoError(t, err)
			require.Equal(t, helloMD5Hex, obj.ETag)
		})
	}
}

// TestValidateContentMD5 exercises the guard directly, including the defensive
// case where clientHex is set but computedHex is empty (needsMD5 invariant broken).
func TestValidateContentMD5(t *testing.T) {
	for _, tt := range []struct {
		name     string
		computed string
		client   string
		wantErr  bool
	}{
		{"no header", "", "", false},
		{"match", "abc123", "abc123", false},
		{"mismatch", "abc123", "wrong", true},
		{"computed empty client set", "", "deadbeef", true}, // defensive: invariant broken
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := validateContentMD5(tt.computed, tt.client)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
