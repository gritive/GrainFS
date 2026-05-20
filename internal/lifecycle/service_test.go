package lifecycle

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/scrubber"
)

// fakeProposer captures the last propose call for assertions.
type fakeProposer struct {
	putCalls []struct {
		Bucket string
		Raw    []byte
	}
	deleteCalls []string
	err         error
}

func (f *fakeProposer) ProposeLifecyclePut(ctx context.Context, bucket string, raw []byte) error {
	f.putCalls = append(f.putCalls, struct {
		Bucket string
		Raw    []byte
	}{bucket, append([]byte(nil), raw...)})
	return f.err
}
func (f *fakeProposer) ProposeLifecycleDelete(ctx context.Context, bucket string) error {
	f.deleteCalls = append(f.deleteCalls, bucket)
	return f.err
}

type fakeLeadership struct {
	leader bool
}

func (f *fakeLeadership) IsLeader() bool { return f.leader }
func (f *fakeLeadership) Subscribe() (<-chan struct{}, func()) {
	ch := make(chan struct{})
	return ch, func() {}
}

func newServiceForTest(t *testing.T) *Service {
	t.Helper()
	return NewService(NewStore(newTestDB(t)), &fakeProposer{}, &fakeLeadership{}, nil, nil, 0)
}

func TestService_Enabled_True(t *testing.T) {
	svc := newServiceForTest(t)
	assert.True(t, svc.Enabled())
}

func TestService_Get_NotFound_ReturnsNil(t *testing.T) {
	svc := newServiceForTest(t)
	cfg, err := svc.Get("nope")
	require.NoError(t, err)
	assert.Nil(t, cfg)
}

func TestService_GetRaw_NotFound_ReturnsNil(t *testing.T) {
	svc := newServiceForTest(t)
	raw, err := svc.GetRaw("nope")
	require.NoError(t, err)
	assert.Nil(t, raw)
}

func TestService_Apply_ValidXML_CallsProposer(t *testing.T) {
	prop := &fakeProposer{}
	svc := NewService(NewStore(newTestDB(t)), prop, &fakeLeadership{}, nil, nil, 0)
	raw := []byte(`<LifecycleConfiguration><Rule><ID>r1</ID><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>`)
	require.NoError(t, svc.Apply(context.Background(), "b", raw))
	require.Len(t, prop.putCalls, 1)
	assert.Equal(t, "b", prop.putCalls[0].Bucket)
	assert.Equal(t, raw, prop.putCalls[0].Raw)
}

func TestService_Apply_InvalidXML_ReturnsError(t *testing.T) {
	prop := &fakeProposer{}
	svc := NewService(NewStore(newTestDB(t)), prop, &fakeLeadership{}, nil, nil, 0)
	err := svc.Apply(context.Background(), "b", []byte("not xml at all"))
	require.Error(t, err)
	assert.Empty(t, prop.putCalls, "proposer must not be called on invalid XML")
}

func TestService_Apply_FailsValidation(t *testing.T) {
	prop := &fakeProposer{}
	svc := NewService(NewStore(newTestDB(t)), prop, &fakeLeadership{}, nil, nil, 0)
	raw := []byte(`<LifecycleConfiguration></LifecycleConfiguration>`)
	err := svc.Apply(context.Background(), "b", raw)
	require.Error(t, err)
	assert.Empty(t, prop.putCalls)
}

func TestService_Delete_CallsProposer(t *testing.T) {
	prop := &fakeProposer{}
	svc := NewService(NewStore(newTestDB(t)), prop, &fakeLeadership{}, nil, nil, 0)
	require.NoError(t, svc.Delete(context.Background(), "b"))
	require.Equal(t, []string{"b"}, prop.deleteCalls)
}

// signalLeadership emits leader-change events on demand. Implements
// LeadershipSignal.
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
	cancel := func() {} // tests do not exercise unsubscribe
	return ch, cancel
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

func TestService_Status_NotRunning(t *testing.T) {
	be := &mockBackend{buckets: []string{}}
	del := &mockDeleter{}
	lead := &signalLeadership{leader: false}
	s := NewService(NewStore(newTestDB(t)), &fakeProposer{}, lead, be, del, 20*time.Millisecond)
	st := s.Status()
	assert.False(t, st.Running)
	assert.True(t, st.LastRun.IsZero())
	assert.Zero(t, st.ObjectsChecked)
	assert.Zero(t, st.Expired)
	assert.Zero(t, st.VersionsPruned)
	assert.Empty(t, st.Buckets)
}

func TestService_Status_IncludesBuckets(t *testing.T) {
	s := newServiceForTest(t)
	require.NoError(t, s.store.PutRaw("b1", []byte("<x/>")))
	require.NoError(t, s.store.PutRaw("b2", []byte("<x/>")))
	st := s.Status()
	assert.ElementsMatch(t, []string{"b1", "b2"}, st.Buckets)
}

func TestService_Status_RunningReflectsWorker(t *testing.T) {
	be := &mockBackend{buckets: []string{}}
	del := &mockDeleter{}
	lead := &signalLeadership{leader: false}
	s := NewService(NewStore(newTestDB(t)), &fakeProposer{}, lead, be, del, 20*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)
	lead.set(true) // become leader → executor starts
	require.Eventually(t, func() bool { return s.WorkerRunningForTest() }, 2*time.Second, 10*time.Millisecond)
	st := s.Status()
	assert.True(t, st.Running)
	lead.set(false) // become follower → executor stops
	require.Eventually(t, func() bool { return !s.WorkerRunningForTest() }, 2*time.Second, 10*time.Millisecond)
	assert.False(t, s.Status().Running)
}

// fakeProposerWithStore simulates a synchronous FSM apply: ProposeLifecyclePut
// writes directly to the store, letting tests verify the Apply→Worker round-trip
// without spinning up a real Raft node.
type fakeProposerWithStore struct {
	store *Store
}

func (f *fakeProposerWithStore) ProposeLifecyclePut(_ context.Context, bucket string, raw []byte) error {
	return f.store.PutRaw(bucket, raw)
}
func (f *fakeProposerWithStore) ProposeLifecycleDelete(_ context.Context, bucket string) error {
	return f.store.Delete(bucket)
}

// TestService_Apply_ThenWorkerProcesses verifies the full module invariant:
// a lifecycle config written via Apply() is eventually processed by the Worker.
func TestService_Apply_ThenWorkerProcesses(t *testing.T) {
	db := newTestDB(t)
	store := NewStore(db)
	prop := &fakeProposerWithStore{store: store}

	oldTime := time.Now().Add(-2 * 24 * time.Hour).Unix()
	be := &mockBackend{
		buckets: []string{"b"},
		objects: map[string][]scrubber.ObjectRecord{
			"b": {{Bucket: "b", Key: "old.log", DataShards: 4, LastModified: oldTime}},
		},
	}
	del := &mockDeleter{}
	lead := &signalLeadership{leader: false}

	svc := NewService(store, prop, lead, be, del, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { svc.Run(ctx); close(runDone) }()

	// Apply config via service (simulates FSM: writes to store via PutRaw).
	raw := []byte(`<LifecycleConfiguration><Rule><ID>r</ID><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>`)
	require.NoError(t, svc.Apply(context.Background(), "b", raw))

	// Become leader → Worker starts and processes the config.
	lead.set(true)
	require.Eventually(t, func() bool {
		del.mu.Lock()
		defer del.mu.Unlock()
		return len(del.deleted) > 0
	}, 2*time.Second, 10*time.Millisecond, "worker should delete expired object after Apply")

	// Stop the service and wait for Run to fully exit (including workerWG.Wait) before
	// asserting, so no worker cycle can race with the assertion. The worker may have
	// run more than one cycle, so check Contains rather than exact-slice equality.
	cancel()
	<-runDone
	del.mu.Lock()
	assert.Contains(t, del.deleted, "b/old.log")
	del.mu.Unlock()
}

func TestService_Run_StartsWorkerOnLeader_StopsOnFollower(t *testing.T) {
	lead := &signalLeadership{leader: false}
	be := &mockBackend{buckets: []string{}} // existing in worker_test.go
	del := &mockDeleter{}                   // existing in worker_test.go
	svc := NewService(NewStore(newTestDB(t)), &fakeProposer{}, lead, be, del, 20*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { svc.Run(ctx); close(done) }()

	// Initially follower: worker must not be running.
	time.Sleep(40 * time.Millisecond)
	assert.False(t, svc.WorkerRunningForTest())

	lead.set(true)
	require.Eventually(t, svc.WorkerRunningForTest, 200*time.Millisecond, 5*time.Millisecond, "worker should start on leader")

	lead.set(false)
	require.Eventually(t, func() bool { return !svc.WorkerRunningForTest() }, 200*time.Millisecond, 5*time.Millisecond, "worker should stop on follower")

	cancel()
	<-done
}

// TestService_MPUWorkerStartsOnFollower asserts the per-node MPU worker runs
// even when the node is a follower (split execution model). The object-side
// worker must remain leader-only.
func TestService_MPUWorkerStartsOnFollower(t *testing.T) {
	be := &mockBackend{buckets: []string{}}
	del := &mockDeleter{}
	lead := &signalLeadership{leader: false}
	svc := NewService(NewStore(newTestDB(t)), &fakeProposer{}, lead, be, del, 20*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { svc.Run(ctx); close(done) }()

	require.Eventually(t, svc.MPUWorkerRunningForTest, 2*time.Second, 10*time.Millisecond,
		"MPU worker must run on followers (split execution model)")
	assert.False(t, svc.WorkerRunningForTest(),
		"object-side worker must NOT run on followers")

	cancel()
	<-done
}

// TestService_BothWorkersStartOnLeader asserts both workers run when the node
// is the leader.
func TestService_BothWorkersStartOnLeader(t *testing.T) {
	be := &mockBackend{buckets: []string{}}
	del := &mockDeleter{}
	lead := &signalLeadership{leader: true}
	svc := NewService(NewStore(newTestDB(t)), &fakeProposer{}, lead, be, del, 20*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { svc.Run(ctx); close(done) }()

	require.Eventually(t, svc.MPUWorkerRunningForTest, 2*time.Second, 10*time.Millisecond,
		"MPU worker must run on every node")
	require.Eventually(t, svc.WorkerRunningForTest, 2*time.Second, 10*time.Millisecond,
		"object-side worker must run on leader")

	cancel()
	<-done
}
