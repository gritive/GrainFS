package migration

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// fakeProposer captures ProposeJob* calls for assertions.
type fakeProposer struct {
	mu      sync.Mutex
	started []string
	done    []struct {
		bucket         string
		copied, errors int64
	}
	failed []struct {
		bucket, reason string
		errors         int64
	}
	err error
}

func (f *fakeProposer) ProposeJobStart(_ context.Context, bucket string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.started = append(f.started, bucket)
	return f.err
}
func (f *fakeProposer) ProposeJobDone(_ context.Context, bucket string, copied, errors int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.done = append(f.done, struct {
		bucket         string
		copied, errors int64
	}{bucket, copied, errors})
	return f.err
}
func (f *fakeProposer) ProposeJobFailed(_ context.Context, bucket, reason string, errors int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed = append(f.failed, struct {
		bucket, reason string
		errors         int64
	}{bucket, reason, errors})
	return f.err
}

// fakeLeadership satisfies LeadershipSignal for unit tests.
type fakeLeadership struct{ leader bool }

func (f *fakeLeadership) IsLeader() bool { return f.leader }
func (f *fakeLeadership) Subscribe() (<-chan struct{}, func()) {
	ch := make(chan struct{})
	return ch, func() {}
}

// signalLeadership allows tests to drive leader changes.
type signalLeadership struct {
	mu     sync.Mutex
	leader bool
	subs   []chan struct{}
}

func (s *signalLeadership) IsLeader() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.leader
}
func (s *signalLeadership) Subscribe() (<-chan struct{}, func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch := make(chan struct{}, 4)
	s.subs = append(s.subs, ch)
	return ch, func() {}
}
func (s *signalLeadership) set(leader bool) {
	s.mu.Lock()
	s.leader = leader
	subs := append([]chan struct{}(nil), s.subs...)
	s.mu.Unlock()
	for _, c := range subs {
		select {
		case c <- struct{}{}:
		default:
		}
	}
}

func TestService_SubmitJob_CallsProposer(t *testing.T) {
	prop := &fakeProposer{}
	svc := NewService(NewJobStore(newTestDB(t)), prop, &fakeLeadership{}, nil, nil, 0)
	require.NoError(t, svc.SubmitJob(context.Background(), "b"))
	prop.mu.Lock()
	defer prop.mu.Unlock()
	require.Len(t, prop.started, 1)
	assert.Equal(t, "b", prop.started[0])
}

func TestService_SubmitJob_ProposerError_ReturnsError(t *testing.T) {
	prop := &fakeProposer{err: assert.AnError}
	svc := NewService(NewJobStore(newTestDB(t)), prop, &fakeLeadership{}, nil, nil, 0)
	err := svc.SubmitJob(context.Background(), "b")
	require.Error(t, err)
}

func TestService_Run_StartsWorkerOnLeader_StopsOnFollower(t *testing.T) {
	lead := &signalLeadership{leader: false}
	svc := NewService(NewJobStore(newTestDB(t)), &fakeProposer{}, lead,
		&noopSource{}, &noopDst{}, 20*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { svc.Run(ctx); close(done) }()

	time.Sleep(40 * time.Millisecond)
	assert.False(t, svc.workerRunningForTest())

	lead.set(true)
	require.Eventually(t, svc.workerRunningForTest, 200*time.Millisecond, 5*time.Millisecond)

	lead.set(false)
	require.Eventually(t, func() bool { return !svc.workerRunningForTest() }, 200*time.Millisecond, 5*time.Millisecond)

	cancel()
	<-done
}

// noopSource satisfies Source with empty results; GetObject always errors.
type noopSource struct{}

func (n *noopSource) ListBuckets() ([]string, error)                        { return nil, nil }
func (n *noopSource) ListObjectsPage(_, _ string) ([]string, string, error) { return nil, "", nil }
func (n *noopSource) GetObject(_, _ string) (io.ReadCloser, *storage.Object, error) {
	return nil, nil, storage.ErrObjectNotFound
}

// noopDst satisfies Destination; all writes succeed silently.
type noopDst struct{}

func (n *noopDst) CreateBucket(_ context.Context, _ string) error { return nil }
func (n *noopDst) PutObject(_ context.Context, _, _ string, body io.Reader, _ string) (*storage.Object, error) {
	return &storage.Object{}, nil
}
func (n *noopDst) GetObject(_ context.Context, _, _ string) (io.ReadCloser, *storage.Object, error) {
	return nil, nil, storage.ErrObjectNotFound
}
