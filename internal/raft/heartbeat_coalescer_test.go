package raft

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeSender implements CoalescerSender for unit tests, capturing whatever the
// coalescer hands to SendHeartbeatBatch and exposing it for inspection.
type fakeSender struct {
	mu       sync.Mutex
	sends    []fakeSend
	sendErr  error
	nextID   atomic.Uint64
	peerAddr string
}

type fakeSend struct {
	corrID  uint64
	payload []byte
}

func (f *fakeSender) SendHeartbeatBatch(payload []byte) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sendErr != nil {
		return 0, f.sendErr
	}
	id := f.nextID.Add(1)
	cp := make([]byte, len(payload))
	copy(cp, payload)
	f.sends = append(f.sends, fakeSend{corrID: id, payload: cp})
	return id, nil
}

func (f *fakeSender) PeerAddr() string {
	if f.peerAddr == "" {
		return "127.0.0.1:0"
	}
	return f.peerAddr
}

func (f *fakeSender) lastSend(t *testing.T) fakeSend {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	require.NotEmpty(t, f.sends, "no sends recorded")
	return f.sends[len(f.sends)-1]
}

// fakeArgs returns a minimal entries-empty AE args for tests.
func fakeArgs(term uint64, leader string) *AppendEntriesArgs {
	return &AppendEntriesArgs{
		Term:         term,
		LeaderID:     leader,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 0,
	}
}

func TestCoalescer_BatchFlush_AllReplies(t *testing.T) {
	sender := &fakeSender{}
	hc := NewHeartbeatCoalescer(sender, 50*time.Millisecond)

	const N = 5
	results := make([]chan hbResult, N)
	for i := 0; i < N; i++ {
		results[i] = make(chan hbResult, 1)
	}

	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			reply, err := hc.AppendEntries(ctx, string([]byte{'g', byte('0' + i)}), fakeArgs(uint64(i+1), "leader"))
			results[i] <- hbResult{reply: reply, err: err}
		}(i)
	}

	// Wait for the flush to occur (50ms), then simulate the receiver decoding
	// our batch and sending back a reply batch.
	time.Sleep(80 * time.Millisecond)

	require.Len(t, sender.sends, 1, "expected exactly one batch send")
	send := sender.sends[0]

	items, err := decodeHeartbeatBatch(send.payload)
	require.NoError(t, err)
	require.Len(t, items, N)

	// Build a synthetic reply batch.
	replies := make([]hbReplyItem, N)
	for i, it := range items {
		replies[i] = hbReplyItem{
			groupID: it.groupID,
			reply: &AppendEntriesReply{
				Term:    it.args.Term + 100,
				Success: true,
			},
		}
	}
	replyPayload, err := encodeHeartbeatReplyBatch(replies)
	require.NoError(t, err)

	hc.DispatchReplyBatch(send.corrID, replyPayload)

	wg.Wait()
	for i := 0; i < N; i++ {
		res := <-results[i]
		require.NoError(t, res.err, "call %d", i)
		require.NotNil(t, res.reply, "call %d", i)
		assert.True(t, res.reply.Success)
		assert.Equal(t, uint64(i+1+100), res.reply.Term)
	}
}

func TestCoalescer_EntriesBypass(t *testing.T) {
	hc := NewHeartbeatCoalescer(&fakeSender{}, 50*time.Millisecond)
	args := fakeArgs(1, "leader")
	args.Entries = []LogEntry{{Term: 1, Index: 1, Command: []byte("x")}}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := hc.AppendEntries(ctx, "g0", args)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refusing entries-bearing AE")
}

func TestCoalescer_SendError(t *testing.T) {
	sender := &fakeSender{sendErr: assertError("transport down")}
	hc := NewHeartbeatCoalescer(sender, 20*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, err := hc.AppendEntries(ctx, "g0", fakeArgs(1, "leader"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "transport down")
}

func TestCoalescer_FailAll(t *testing.T) {
	sender := &fakeSender{}
	hc := NewHeartbeatCoalescer(sender, 50*time.Millisecond)

	doneCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := hc.AppendEntries(ctx, "g0", fakeArgs(1, "leader"))
		doneCh <- err
	}()
	// Wait for flush + inflight registration.
	time.Sleep(80 * time.Millisecond)
	hc.FailAll(assertError("conn broken"))

	select {
	case err := <-doneCh:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "conn broken")
	case <-time.After(2 * time.Second):
		t.Fatal("call did not return after FailAll")
	}
}

func TestCoalescer_PartialReply(t *testing.T) {
	sender := &fakeSender{}
	hc := NewHeartbeatCoalescer(sender, 30*time.Millisecond)

	// 2 calls, but only 1 reply in the batch reply.
	const N = 2
	resCh := make([]chan hbResult, N)
	for i := 0; i < N; i++ {
		resCh[i] = make(chan hbResult, 1)
	}
	for i := 0; i < N; i++ {
		go func(i int) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			r, e := hc.AppendEntries(ctx, string([]byte{'g', byte('0' + i)}), fakeArgs(uint64(i+1), "leader"))
			resCh[i] <- hbResult{reply: r, err: e}
		}(i)
	}
	time.Sleep(60 * time.Millisecond)
	require.Len(t, sender.sends, 1)
	items, _ := decodeHeartbeatBatch(sender.sends[0].payload)
	require.Len(t, items, N)

	// Reply only for the first item in the batch (whichever was enqueued first).
	replies := []hbReplyItem{
		{groupID: items[0].groupID, reply: &AppendEntriesReply{Term: 1, Success: true}},
	}
	replyPayload, _ := encodeHeartbeatReplyBatch(replies)
	hc.DispatchReplyBatch(sender.sends[0].corrID, replyPayload)

	// Map groupID → result chan so we don't depend on enqueue order.
	bygid := map[string]chan hbResult{
		"g0": resCh[0],
		"g1": resCh[1],
	}
	repliedGID := items[0].groupID
	missingGID := items[1].groupID
	rOK := <-bygid[repliedGID]
	rGap := <-bygid[missingGID]
	require.NoError(t, rOK.err, "replied group should succeed")
	require.NotNil(t, rOK.reply)
	assert.True(t, rOK.reply.Success)
	require.Error(t, rGap.err)
	assert.ErrorIs(t, rGap.err, ErrNoResponse)
}

func TestCoalescer_FlushWindowTimer(t *testing.T) {
	sender := &fakeSender{}
	hc := NewHeartbeatCoalescer(sender, 30*time.Millisecond)

	start := time.Now()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = hc.AppendEntries(ctx, "g0", fakeArgs(1, "leader"))
	}()
	// Wait for flush.
	for i := 0; i < 200; i++ {
		sender.mu.Lock()
		n := len(sender.sends)
		sender.mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	elapsed := time.Since(start)
	require.Len(t, sender.sends, 1)
	assert.Greater(t, elapsed, 25*time.Millisecond, "flush should be ≥ flush window")
	assert.Less(t, elapsed, 200*time.Millisecond, "flush should not be far above window")
}

func TestCoalescer_HandleBatchOnReceiver(t *testing.T) {
	// Encode a batch ourselves and verify the dispatch helper produces a
	// matching reply batch.
	items := []hbItem{
		{groupID: "g0", args: fakeArgs(1, "L"), replyCh: make(chan hbResult, 1)},
		{groupID: "g1", args: fakeArgs(2, "L"), replyCh: make(chan hbResult, 1)},
	}
	payload, err := encodeHeartbeatBatch(items)
	require.NoError(t, err)

	dispatch := func(groupID string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
		return &AppendEntriesReply{Term: args.Term + 10, Success: true}, nil
	}
	replyBytes := HandleBatchOnReceiver(payload, dispatch)
	require.NotNil(t, replyBytes)

	replies, err := decodeHeartbeatReplyBatch(replyBytes)
	require.NoError(t, err)
	require.Len(t, replies, 2)

	assert.Equal(t, "g0", replies[0].groupID)
	assert.Equal(t, "", replies[0].errStr)
	require.NotNil(t, replies[0].reply)
	assert.Equal(t, uint64(11), replies[0].reply.Term)

	assert.Equal(t, "g1", replies[1].groupID)
	require.NotNil(t, replies[1].reply)
	assert.Equal(t, uint64(12), replies[1].reply.Term)
}

func TestCoalescer_HandleBatchOnReceiver_DispatchError(t *testing.T) {
	items := []hbItem{
		{groupID: "g0", args: fakeArgs(1, "L"), replyCh: make(chan hbResult, 1)},
	}
	payload, _ := encodeHeartbeatBatch(items)
	dispatch := func(groupID string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
		return nil, assertError("group not found")
	}
	replyBytes := HandleBatchOnReceiver(payload, dispatch)
	require.NotNil(t, replyBytes)

	replies, err := decodeHeartbeatReplyBatch(replyBytes)
	require.NoError(t, err)
	require.Len(t, replies, 1)
	assert.Equal(t, "group not found", replies[0].errStr)
	assert.Nil(t, replies[0].reply)
}
