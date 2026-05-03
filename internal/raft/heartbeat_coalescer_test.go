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

// fakeSender implements CoalescerSender for unit tests, capturing what the
// coalescer hands to SendHeartbeatBatchWithCorrID and exposing it for
// inspection. onSend lets a test hook into the wire-side of a flush so it
// can simulate a fast receiver replying mid-send (regression coverage for
// the inFlight.Store-vs-Send ordering bug).
type fakeSender struct {
	mu       sync.Mutex
	sends    []fakeSend
	sendErr  error
	nextID   atomic.Uint64
	peerAddr string
	onSend   func(corrID uint64, payload []byte)
}

type fakeSend struct {
	corrID  uint64
	payload []byte
}

func (f *fakeSender) NextHeartbeatCorrID() uint64 {
	return f.nextID.Add(1)
}

func (f *fakeSender) SendHeartbeatBatchWithCorrID(corrID uint64, payload []byte) error {
	f.mu.Lock()
	if f.sendErr != nil {
		err := f.sendErr
		f.mu.Unlock()
		return err
	}
	cp := make([]byte, len(payload))
	copy(cp, payload)
	f.sends = append(f.sends, fakeSend{corrID: corrID, payload: cp})
	hook := f.onSend
	f.mu.Unlock()
	if hook != nil {
		hook(corrID, cp)
	}
	return nil
}

func (f *fakeSender) PeerAddr() string {
	if f.peerAddr == "" {
		return "127.0.0.1:0"
	}
	return f.peerAddr
}

func (f *fakeSender) sendsSnapshot() []fakeSend {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]fakeSend, len(f.sends))
	copy(out, f.sends)
	return out
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

	sends := sender.sendsSnapshot()
	require.Len(t, sends, 1, "expected exactly one batch send")
	send := sends[0]

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
	sends := sender.sendsSnapshot()
	require.Len(t, sends, 1)
	items, _ := decodeHeartbeatBatch(sends[0].payload)
	require.Len(t, items, N)

	// Reply only for the first item in the batch (whichever was enqueued first).
	replies := []hbReplyItem{
		{groupID: items[0].groupID, reply: &AppendEntriesReply{Term: 1, Success: true}},
	}
	replyPayload, _ := encodeHeartbeatReplyBatch(replies)
	hc.DispatchReplyBatch(sends[0].corrID, replyPayload)

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

// TestCoalescer_FastReplyBeforeRegister covers the race where a reply for
// the corrID arrives before the inflight batch is registered. Pre-fix the
// coalescer called SendHeartbeatBatch first and only then stored the entry,
// so DispatchReplyBatch dropped the reply and the caller hung. Post-fix the
// store happens before send, so even a synchronous in-send dispatch lands
// on a registered entry. The hook simulates the worst case: the receiver
// turns the request around inside SendHeartbeatBatchWithCorrID.
func TestCoalescer_FastReplyBeforeRegister(t *testing.T) {
	var hc *HeartbeatCoalescer
	sender := &fakeSender{
		onSend: func(corrID uint64, payload []byte) {
			items, err := decodeHeartbeatBatch(payload)
			if err != nil {
				return
			}
			replies := make([]hbReplyItem, len(items))
			for i, it := range items {
				replies[i] = hbReplyItem{
					groupID: it.groupID,
					reply:   &AppendEntriesReply{Term: it.args.Term, Success: true},
				}
			}
			replyPayload, err := encodeHeartbeatReplyBatch(replies)
			if err != nil {
				return
			}
			// Dispatch synchronously from inside the wire-level send. If
			// flush() registers AFTER calling Send (the buggy order), the
			// inFlight map will not yet contain corrID and the reply is
			// silently dropped -> caller times out.
			hc.DispatchReplyBatch(corrID, replyPayload)
		},
	}
	hc = NewHeartbeatCoalescer(sender, 1*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	reply, err := hc.AppendEntries(ctx, "g0", fakeArgs(7, "leader"))
	require.NoError(t, err, "reply must be delivered when in-flight is registered before send")
	require.NotNil(t, reply)
	assert.True(t, reply.Success)
	assert.Equal(t, uint64(7), reply.Term)

	require.Len(t, sender.sends, 1)
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
