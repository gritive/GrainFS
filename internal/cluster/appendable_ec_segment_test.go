package cluster

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// fakeSegmentECOpener records the refs it is asked to reconstruct and returns
// canned bytes per BlobID. Stands in for *clusterSegmentStore so the reader's
// EC-vs-plain dispatch is exercised without an EC shard service.
type fakeSegmentECOpener struct {
	bytesByBlob map[string][]byte
	opened      []string
}

func (f *fakeSegmentECOpener) OpenSegment(_ context.Context, ref storage.SegmentRef) (io.ReadCloser, error) {
	f.opened = append(f.opened, ref.BlobID)
	return io.NopCloser(bytes.NewReader(f.bytesByBlob[ref.BlobID])), nil
}

// TestAppendableSegmentReader_ReconstructsECBaseSegment pins the fix for the
// false "open segment ... local missing, peer fetch failed" GET error after
// appending to a plain (chunked-PUT) object. The chunked PUT stores its base
// bytes as EC-backed segments (ECData>0, NodeIDs set) — there is NO plain
// _segments/<blobID> file for them. Appending flips the object to IsAppendable,
// so the GET goes through openAppendableSegments; that reader MUST reconstruct
// the EC base segment via the segment store and only use the local-file path
// for genuine plain append blobs. Before the fix the reader opened every
// segment as a plain file and failed on the EC base segment.
func TestAppendableSegmentReader_ReconstructsECBaseSegment(t *testing.T) {
	dir := t.TempDir()

	// Plain append blob lives on disk as a real file at its segmentBlobPath.
	b := &DistributedBackend{root: dir}
	plainBlobID := "plain-append-blob"
	plainPath := b.segmentBlobPath("bkt", "obj", plainBlobID)
	require.NoError(t, os.MkdirAll(filepath.Dir(plainPath), 0o755))
	require.NoError(t, os.WriteFile(plainPath, []byte("-append"), 0o644))

	ecBlobID := "ec-base-segment"
	fake := &fakeSegmentECOpener{bytesByBlob: map[string][]byte{ecBlobID: []byte("plain")}}

	reader := &appendableSegmentReader{
		backend:  b,
		bucket:   "bkt",
		key:      "obj",
		paths:    []string{b.segmentBlobPath("bkt", "obj", ecBlobID), plainPath},
		blobIDs:  []string{ecBlobID, plainBlobID},
		kinds:    []byte{appendSegKindSegment, appendSegKindSegment},
		ecRefs:   []*storage.CoalescedRef{nil, nil},
		segRefs:  []*storage.SegmentRef{{BlobID: ecBlobID, Size: 5, ECData: 1, NodeIDs: []string{"n0"}}, nil},
		segStore: fake,
	}

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "plain-append", string(got),
		"EC base segment must be reconstructed via the store and stitched with the plain append blob")
	require.Equal(t, []string{ecBlobID}, fake.opened,
		"only the EC-backed segment should route through the segment store")
}

// TestSegmentRefIsECBacked pins the discriminator between an EC base segment
// (chunked-PUT bytes) and a plain append blob.
func TestSegmentRefIsECBacked(t *testing.T) {
	require.True(t, segmentRefIsECBacked(storage.SegmentRef{ECData: 1, NodeIDs: []string{"n0"}}),
		"a segment with EC params and nodes is EC-backed")
	require.False(t, segmentRefIsECBacked(storage.SegmentRef{BlobID: "b", Size: 7}),
		"a plain append blob (BlobID+Size only) is not EC-backed")
	require.False(t, segmentRefIsECBacked(storage.SegmentRef{ECData: 1}),
		"EC params without nodes cannot be reconstructed — not EC-backed")
}
