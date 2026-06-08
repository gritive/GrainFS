package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingLookup counts calls so we can prove the coordinator's OpRouter read
// path goes through the injected reader, not the meta adapter.
type recordingLookup struct {
	calls int
	entry ObjectIndexEntry
	ok    bool
}

func (r *recordingLookup) ObjectIndexLatest(bucket, key string) (ObjectIndexEntry, bool) {
	r.calls++
	return r.entry, r.ok
}
func (r *recordingLookup) ObjectIndexVersion(bucket, key, versionID string) (ObjectIndexEntry, bool) {
	r.calls++
	return r.entry, r.ok
}

// TestCoordinator_WithObjectIndexReader_RoutesOpRouterReads proves that after
// WithObjectIndexReader, the coordinator's live OpRouter (from runtimeState())
// routes object-index point-reads through the injected reader.
//
// Note: c.opRouter is set only on the first rebuild (NewClusterCoordinator),
// and subsequent builder calls update only the atomic runtime pointer. The
// production read path therefore uses c.runtimeState().opRouter, and the test
// must mirror that — calling c.opRouter directly would observe the pre-injection
// router and the reader would never be reached.
func TestCoordinator_WithObjectIndexReader_RoutesOpRouterReads(t *testing.T) {
	reader := &recordingLookup{entry: ObjectIndexEntry{Bucket: "b", Key: "k", VersionID: "v"}, ok: true}
	c := NewClusterCoordinator(nil, nil, nil, nil, "self").
		WithObjectIndexReader(reader)
	liveRouter := c.runtimeState().opRouter
	require.NotNil(t, liveRouter)
	_, _, err := liveRouter.RouteObjectRead("b", "k", "")
	_ = err // routing may fail (no groups); we only assert the reader was consulted
	assert.Equal(t, 1, reader.calls, "OpRouter read must go through the injected index reader")
}

// TestCoordinator_NoReader_FallsBackToMetaAdapter proves that without an
// injected reader, the coordinator falls back to metaObjectIndexAdapter(meta),
// which with a nil meta returns nil — yielding ErrObjectIndexRequired.
func TestCoordinator_NoReader_FallsBackToMetaAdapter(t *testing.T) {
	c := NewClusterCoordinator(nil, nil, nil, nil, "self")
	liveRouter := c.runtimeState().opRouter
	require.NotNil(t, liveRouter)
	_, _, err := liveRouter.RouteObjectRead("b", "k", "")
	require.Error(t, err) // index nil → ErrObjectIndexRequired, unchanged
}

// recordingListReader embeds recordingLookup and additionally records LIST calls.
type recordingListReader struct {
	recordingLookup
	pageCalls int
}

func (r *recordingListReader) ObjectIndexLatestEntriesPage(bucket, prefix, marker string, maxKeys int) ([]ObjectIndexEntry, bool) {
	r.pageCalls++
	return nil, false
}
func (r *recordingListReader) ObjectIndexLatestEntries(bucket, prefix string, maxKeys int) []ObjectIndexEntry {
	return nil
}
func (r *recordingListReader) ObjectIndexVersionEntries(bucket, prefix string, maxKeys int) []ObjectIndexEntry {
	return nil
}

// stubProposer is a minimal objectIndexProposer that satisfies the interface.
type stubProposer struct{}

func (stubProposer) ProposeObjectIndex(_ context.Context, _ ObjectIndexEntry, _ bool) error {
	return nil
}
func (stubProposer) ProposeDeleteObjectIndex(_ context.Context, _, _, _ string) error { return nil }

// TestCoordinator_ListSource_PrefersInjectedReader proves that objectIndexListSource()
// returns the injected reader's LIST implementation rather than c.meta's.
func TestCoordinator_ListSource_PrefersInjectedReader(t *testing.T) {
	r := &recordingListReader{}
	c := NewClusterCoordinator(nil, nil, nil, nil, "self").
		WithObjectIndexProposer(stubProposer{}). // indexWriter non-nil so objectIndexListSource() is enabled
		WithObjectIndexReader(r)
	src, ok := c.objectIndexListSource()
	require.True(t, ok)
	require.NotNil(t, src)
	_, _ = src.ObjectIndexLatestEntriesPage("b", "", "", 0)
	assert.Equal(t, 1, r.pageCalls, "LIST must go through the injected façade reader")
}
