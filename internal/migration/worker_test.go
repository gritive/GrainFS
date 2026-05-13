package migration

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// mockSource is a pageable source for worker tests.
type mockSource struct {
	mu      sync.Mutex
	buckets []string
	pages   map[string][][]string // bucket → pages of keys
}

func (m *mockSource) ListBuckets() ([]string, error) { return m.buckets, nil }

func (m *mockSource) ListObjectsPage(bucket, cursor string) ([]string, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pages := m.pages[bucket]
	idx := 0
	if cursor != "" {
		for i, p := range pages {
			// cursor is the last key of the previous page
			if len(p) > 0 && p[len(p)-1] == cursor {
				idx = i + 1
				break
			}
		}
	}
	if idx >= len(pages) {
		return nil, "", nil
	}
	keys := pages[idx]
	next := ""
	if idx+1 < len(pages) {
		next = keys[len(keys)-1] // use last key of current page as cursor
	}
	return keys, next, nil
}

func (m *mockSource) GetObject(bucket, key string) (io.ReadCloser, *storage.Object, error) {
	return io.NopCloser(strings.NewReader("content")), &storage.Object{Key: key}, nil
}

// mockDst records PutObject calls.
type mockDst struct {
	mu      sync.Mutex
	puts    []string
	created []string
}

func (d *mockDst) CreateBucket(_ context.Context, bucket string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.created = append(d.created, bucket)
	return nil
}

func (d *mockDst) PutObject(_ context.Context, bucket, key string, body io.Reader, _ string) (*storage.Object, error) {
	_, _ = io.Copy(io.Discard, body)
	d.mu.Lock()
	defer d.mu.Unlock()
	d.puts = append(d.puts, bucket+"/"+key)
	return &storage.Object{Key: key}, nil
}

func (d *mockDst) GetObject(_ context.Context, _, _ string) (io.ReadCloser, *storage.Object, error) {
	return nil, nil, storage.ErrObjectNotFound
}

func TestWorker_ProcessJob_CopiesObjects_ProposesJobDone(t *testing.T) {
	db := newTestDB(t)
	store := NewJobStore(db)
	require.NoError(t, store.SaveJob(&JobState{Bucket: "b", Status: StatusRunning}))

	src := &mockSource{
		buckets: []string{"b"},
		pages:   map[string][][]string{"b": {{"a.txt", "b.txt"}}},
	}
	dst := &mockDst{}
	prop := &fakeProposer{}

	w := newWorker(store, src, dst, prop, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	w.processAll(ctx)

	dst.mu.Lock()
	assert.ElementsMatch(t, []string{"b/a.txt", "b/b.txt"}, dst.puts)
	dst.mu.Unlock()

	prop.mu.Lock()
	require.Len(t, prop.done, 1)
	assert.Equal(t, "b", prop.done[0].bucket)
	assert.Equal(t, int64(2), prop.done[0].copied)
	assert.Equal(t, int64(0), prop.done[0].errors)
	prop.mu.Unlock()
}

func TestWorker_ProcessJob_SavesCursorAfterLastPage(t *testing.T) {
	db := newTestDB(t)
	store := NewJobStore(db)
	require.NoError(t, store.SaveJob(&JobState{Bucket: "b", Status: StatusRunning}))

	src := &mockSource{
		buckets: []string{"b"},
		// Two pages: cursor after page 0 is "b.txt".
		pages: map[string][][]string{"b": {{"a.txt", "b.txt"}, {"c.txt"}}},
	}
	dst := &mockDst{}
	prop := &fakeProposer{}

	w := newWorker(store, src, dst, prop, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	w.processAll(ctx)

	// After completion, cursor should be "" (last page had empty next).
	cur, err := store.GetCursor("b")
	require.NoError(t, err)
	assert.Empty(t, cur)

	prop.mu.Lock()
	assert.Len(t, prop.done, 1)
	assert.Equal(t, int64(3), prop.done[0].copied)
	prop.mu.Unlock()
}

func TestWorker_Run_ProcessesJobOnTrigger(t *testing.T) {
	db := newTestDB(t)
	store := NewJobStore(db)
	prop := &fakeProposer{}
	// Configure source and save job BEFORE starting the worker to avoid
	// a race where the startup processAll runs before the job/src are ready.
	src := &mockSource{
		buckets: []string{"b"},
		pages:   map[string][][]string{"b": {{"x.txt"}}},
	}
	dst := &mockDst{}
	require.NoError(t, store.SaveJob(&JobState{Bucket: "b", Status: StatusRunning}))

	w := newWorker(store, src, dst, prop, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	// Worker processes the job on startup — no Trigger call needed.
	require.Eventually(t, func() bool {
		prop.mu.Lock()
		defer prop.mu.Unlock()
		return len(prop.done) > 0
	}, 2*time.Second, 10*time.Millisecond)

	prop.mu.Lock()
	assert.Equal(t, "b", prop.done[0].bucket)
	assert.Equal(t, int64(1), prop.done[0].copied)
	prop.mu.Unlock()
}

func TestWorker_Run_ResumesRunningJobsOnStart(t *testing.T) {
	db := newTestDB(t)
	store := NewJobStore(db)
	require.NoError(t, store.SaveJob(&JobState{Bucket: "pre", Status: StatusRunning}))

	src := &mockSource{
		buckets: []string{"pre"},
		pages:   map[string][][]string{"pre": {{"file.txt"}}},
	}
	dst := &mockDst{}
	prop := &fakeProposer{}

	w := newWorker(store, src, dst, prop, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	// Worker must process the pre-existing running job without a Trigger call.
	require.Eventually(t, func() bool {
		prop.mu.Lock()
		defer prop.mu.Unlock()
		return len(prop.done) > 0
	}, 2*time.Second, 10*time.Millisecond)
}

// errorSource returns an error on ListObjectsPage for testing ProposeJobFailed.
type errorSource struct{ listErr error }

func (e *errorSource) ListBuckets() ([]string, error) { return nil, nil }
func (e *errorSource) ListObjectsPage(_, _ string) ([]string, string, error) {
	return nil, "", e.listErr
}
func (e *errorSource) GetObject(_, _ string) (io.ReadCloser, *storage.Object, error) {
	return nil, nil, e.listErr
}

func TestWorker_ProposeJobFailed_OnListObjectsPageError(t *testing.T) {
	db := newTestDB(t)
	store := NewJobStore(db)
	require.NoError(t, store.SaveJob(&JobState{Bucket: "b", Status: StatusRunning}))

	src := &errorSource{listErr: errors.New("source unavailable")}
	dst := &mockDst{}
	prop := &fakeProposer{}

	w := newWorker(store, src, dst, prop, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	w.processAll(ctx)

	prop.mu.Lock()
	require.Len(t, prop.failed, 1, "ProposeJobFailed must be called on ListObjectsPage error")
	assert.Equal(t, "b", prop.failed[0].bucket)
	assert.Contains(t, prop.failed[0].reason, "source unavailable")
	prop.mu.Unlock()
}
