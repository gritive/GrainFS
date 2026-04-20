package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/transport"
)

// mockCaller lets tests pin each peer's RPC response. The peerResp map is
// consulted first; if no explicit response is set, peerErr fires instead.
type mockCaller struct {
	mu        sync.Mutex
	peerResp  map[string]*transport.Message
	peerErr   map[string]error
	peerDelay map[string]time.Duration
	callCount atomic.Int32
}

func newMockCaller() *mockCaller {
	return &mockCaller{
		peerResp:  make(map[string]*transport.Message),
		peerErr:   make(map[string]error),
		peerDelay: make(map[string]time.Duration),
	}
}

func (m *mockCaller) setResp(peer string, resp *transport.Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.peerResp[peer] = resp
}

func (m *mockCaller) setErr(peer string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.peerErr[peer] = err
}

func (m *mockCaller) setDelay(peer string, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.peerDelay[peer] = d
}

func (m *mockCaller) Call(ctx context.Context, addr string, req *transport.Message) (*transport.Message, error) {
	m.callCount.Add(1)
	m.mu.Lock()
	delay := m.peerDelay[addr]
	err := m.peerErr[addr]
	resp := m.peerResp[addr]
	m.mu.Unlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// buildQueryResponse constructs a FB-encoded ReceiptQueryResponseMsg for tests.
func buildQueryResponse(t *testing.T, found bool, receiptJSON []byte) *transport.Message {
	t.Helper()
	b := flatbuffers.NewBuilder(128)
	var jsonOff flatbuffers.UOffsetT
	if len(receiptJSON) > 0 {
		jsonOff = b.CreateByteVector(receiptJSON)
	}
	clusterpb.ReceiptQueryResponseMsgStart(b)
	clusterpb.ReceiptQueryResponseMsgAddFound(b, found)
	if len(receiptJSON) > 0 {
		clusterpb.ReceiptQueryResponseMsgAddReceiptJson(b, jsonOff)
	}
	b.Finish(clusterpb.ReceiptQueryResponseMsgEnd(b))
	raw := b.FinishedBytes()
	out := make([]byte, len(raw))
	copy(out, raw)
	return &transport.Message{Type: transport.StreamReceiptQuery, Payload: out}
}

func TestReceiptBroadcaster_Query_FirstSuccessReturns(t *testing.T) {
	caller := newMockCaller()
	peers := []string{"peer-a", "peer-b", "peer-c"}

	// peer-b holds the receipt.
	receiptJSON := []byte(`{"receipt_id":"rcpt-42"}`)
	caller.setResp("peer-a", buildQueryResponse(t, false, nil))
	caller.setResp("peer-b", buildQueryResponse(t, true, receiptJSON))
	caller.setResp("peer-c", buildQueryResponse(t, false, nil))

	b := NewReceiptBroadcaster(caller, peers, 3*time.Second)
	ctx := context.Background()

	got, found, err := b.Query(ctx, "rcpt-42")
	require.NoError(t, err)
	require.True(t, found)
	assert.JSONEq(t, string(receiptJSON), string(got))
}

func TestReceiptBroadcaster_Query_NoneHaveReceipt(t *testing.T) {
	caller := newMockCaller()
	peers := []string{"peer-a", "peer-b"}
	caller.setResp("peer-a", buildQueryResponse(t, false, nil))
	caller.setResp("peer-b", buildQueryResponse(t, false, nil))

	b := NewReceiptBroadcaster(caller, peers, 3*time.Second)

	_, found, err := b.Query(context.Background(), "rcpt-nonexistent")
	require.NoError(t, err)
	assert.False(t, found)
}

func TestReceiptBroadcaster_Query_TimeoutReturnsError(t *testing.T) {
	caller := newMockCaller()
	peers := []string{"peer-slow"}
	caller.setResp("peer-slow", buildQueryResponse(t, true, []byte(`{}`)))
	caller.setDelay("peer-slow", 2*time.Second) // exceeds our timeout

	b := NewReceiptBroadcaster(caller, peers, 50*time.Millisecond)

	start := time.Now()
	_, _, err := b.Query(context.Background(), "rcpt-42")
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, elapsed, 500*time.Millisecond, "must honor timeout, not peer delay")
}

func TestReceiptBroadcaster_Query_OnePeerFailsOthersSucceed(t *testing.T) {
	caller := newMockCaller()
	peers := []string{"peer-dead", "peer-live"}

	receiptJSON := []byte(`{"receipt_id":"rcpt-99"}`)
	caller.setErr("peer-dead", errors.New("connection refused"))
	caller.setResp("peer-live", buildQueryResponse(t, true, receiptJSON))

	b := NewReceiptBroadcaster(caller, peers, 3*time.Second)

	got, found, err := b.Query(context.Background(), "rcpt-99")
	require.NoError(t, err)
	require.True(t, found)
	assert.JSONEq(t, string(receiptJSON), string(got))
}

func TestReceiptBroadcaster_Query_NoPeers(t *testing.T) {
	b := NewReceiptBroadcaster(newMockCaller(), nil, 3*time.Second)

	_, found, err := b.Query(context.Background(), "rcpt-x")
	require.NoError(t, err)
	assert.False(t, found, "zero peers = not found, not error")
}

func TestReceiptBroadcaster_Query_AllPeersFail(t *testing.T) {
	caller := newMockCaller()
	peers := []string{"peer-a", "peer-b"}
	caller.setErr("peer-a", errors.New("dial failure"))
	caller.setErr("peer-b", errors.New("stream closed"))

	b := NewReceiptBroadcaster(caller, peers, 3*time.Second)

	_, found, err := b.Query(context.Background(), "rcpt-x")
	require.NoError(t, err, "all-peer failure is a not-found, not an error")
	assert.False(t, found)
}

func TestReceiptBroadcaster_Query_CancelsRemainingAfterFirstSuccess(t *testing.T) {
	caller := newMockCaller()
	peers := []string{"peer-fast", "peer-slow-1", "peer-slow-2"}

	receiptJSON := []byte(`{"receipt_id":"rcpt-quick"}`)
	caller.setResp("peer-fast", buildQueryResponse(t, true, receiptJSON))
	// Slow peers would block if we didn't cancel them.
	caller.setResp("peer-slow-1", buildQueryResponse(t, false, nil))
	caller.setResp("peer-slow-2", buildQueryResponse(t, false, nil))
	caller.setDelay("peer-slow-1", 2*time.Second)
	caller.setDelay("peer-slow-2", 2*time.Second)

	b := NewReceiptBroadcaster(caller, peers, 3*time.Second)

	start := time.Now()
	got, found, err := b.Query(context.Background(), "rcpt-quick")
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.True(t, found)
	assert.JSONEq(t, string(receiptJSON), string(got))
	assert.Less(t, elapsed, 500*time.Millisecond, "slow peers must be cancelled once fast peer answered")
}

// --- Server-side handler tests ---

// fakeReceiptLookup is a ReceiptLookup implementation for handler tests.
type fakeReceiptLookup struct {
	data map[string][]byte // id → JSON
}

func (f *fakeReceiptLookup) LookupReceiptJSON(id string) ([]byte, bool) {
	v, ok := f.data[id]
	return v, ok
}

func TestReceiptQueryHandler_ReturnsReceiptWhenFound(t *testing.T) {
	lookup := &fakeReceiptLookup{data: map[string][]byte{
		"rcpt-here": []byte(`{"receipt_id":"rcpt-here","payload":"x"}`),
	}}

	handler := NewReceiptQueryHandler(lookup)

	// Build a request.
	b := flatbuffers.NewBuilder(64)
	idOff := b.CreateString("rcpt-here")
	clusterpb.ReceiptQueryMsgStart(b)
	clusterpb.ReceiptQueryMsgAddReceiptId(b, idOff)
	b.Finish(clusterpb.ReceiptQueryMsgEnd(b))
	req := &transport.Message{Type: transport.StreamReceiptQuery, Payload: b.FinishedBytes()}

	resp := handler(req)
	require.NotNil(t, resp)
	require.Equal(t, transport.StreamReceiptQuery, resp.Type)

	parsed := clusterpb.GetRootAsReceiptQueryResponseMsg(resp.Payload, 0)
	assert.True(t, parsed.Found())
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(parsed.ReceiptJsonBytes(), &decoded))
	assert.Equal(t, "rcpt-here", decoded["receipt_id"])
}

func TestReceiptQueryHandler_ReturnsNotFound(t *testing.T) {
	lookup := &fakeReceiptLookup{data: map[string][]byte{}}
	handler := NewReceiptQueryHandler(lookup)

	b := flatbuffers.NewBuilder(64)
	idOff := b.CreateString("rcpt-missing")
	clusterpb.ReceiptQueryMsgStart(b)
	clusterpb.ReceiptQueryMsgAddReceiptId(b, idOff)
	b.Finish(clusterpb.ReceiptQueryMsgEnd(b))
	req := &transport.Message{Type: transport.StreamReceiptQuery, Payload: b.FinishedBytes()}

	resp := handler(req)
	require.NotNil(t, resp)
	parsed := clusterpb.GetRootAsReceiptQueryResponseMsg(resp.Payload, 0)
	assert.False(t, parsed.Found())
	assert.Empty(t, parsed.ReceiptJsonBytes())
}

func TestReceiptQueryHandler_RejectsInvalidPayload(t *testing.T) {
	lookup := &fakeReceiptLookup{}
	handler := NewReceiptQueryHandler(lookup)

	req := &transport.Message{Type: transport.StreamReceiptQuery, Payload: []byte{0xff, 0xff, 0xff, 0xff}}
	resp := handler(req)
	// Invalid request → respond with found=false rather than panic/crash.
	require.NotNil(t, resp)
	parsed := clusterpb.GetRootAsReceiptQueryResponseMsg(resp.Payload, 0)
	assert.False(t, parsed.Found())
}
