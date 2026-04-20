package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/transport"
)

// Caller is the narrow subset of transport.QUICTransport the broadcaster
// uses. Declared as an interface so tests inject a mock without spinning up
// a real QUIC listener.
type Caller interface {
	Call(ctx context.Context, addr string, req *transport.Message) (*transport.Message, error)
}

// ReceiptLookup is the server-side dependency of the query handler — the
// broadcaster peer looks up the receipt in its local store and returns
// the signed JSON. Implementations live in internal/receipt.
type ReceiptLookup interface {
	LookupReceiptJSON(id string) ([]byte, bool)
}

// ReceiptBroadcaster fans out a "do you have this receipt?" query to every
// peer in parallel and returns the first affirmative response. Used as the
// fallback path when the in-memory RoutingCache does not know which node
// holds a receipt (window evicted it).
//
// Semantics: first-success within timeout wins. Remaining in-flight peers
// are cancelled via context once the first winner reports. If every peer
// responds "not found" or every peer fails, the broadcaster returns
// (nil, false, nil) — a miss is expected and not an error.
type ReceiptBroadcaster struct {
	caller  Caller
	peers   []string
	timeout time.Duration
	logger  *slog.Logger

	// Observability hooks (S6). Nil-safe — counters only fire when the
	// broadcaster is wired to real metrics.
	metrics BroadcastMetricsRecorder
}

// BroadcastMetricsRecorder receives partial-success / timeout / hit counters.
// Implemented by a Prometheus-backed recorder (S6); tests use a no-op.
type BroadcastMetricsRecorder interface {
	OnBroadcastStart()
	OnBroadcastHit()
	OnBroadcastMiss()
	OnBroadcastTimeout()
	OnBroadcastPartialSuccess(responded, total int)
}

type noopMetrics struct{}

func (noopMetrics) OnBroadcastStart()                   {}
func (noopMetrics) OnBroadcastHit()                     {}
func (noopMetrics) OnBroadcastMiss()                    {}
func (noopMetrics) OnBroadcastTimeout()                 {}
func (noopMetrics) OnBroadcastPartialSuccess(int, int)  {}

// NewReceiptBroadcaster builds a broadcaster. peers is the list of cluster
// members to query (caller should exclude the local node address); timeout
// is the per-query hard deadline (Slice 2 target: 3s).
func NewReceiptBroadcaster(caller Caller, peers []string, timeout time.Duration) *ReceiptBroadcaster {
	return &ReceiptBroadcaster{
		caller:  caller,
		peers:   peers,
		timeout: timeout,
		logger:  slog.Default().With("component", "receipt-broadcast"),
		metrics: noopMetrics{},
	}
}

// SetMetrics wires a real metrics recorder. nil switches back to no-op.
func (b *ReceiptBroadcaster) SetMetrics(m BroadcastMetricsRecorder) {
	if m == nil {
		b.metrics = noopMetrics{}
		return
	}
	b.metrics = m
}

// Query asks every peer in parallel for the given receipt ID. Returns
// (receipt JSON, true, nil) on first affirmative response; (nil, false, nil)
// if every peer said "not found" or failed; (nil, false, ctx.Err()) when
// the timeout elapsed before any peer answered affirmatively.
func (b *ReceiptBroadcaster) Query(ctx context.Context, receiptID string) ([]byte, bool, error) {
	b.metrics.OnBroadcastStart()

	if len(b.peers) == 0 {
		b.metrics.OnBroadcastMiss()
		return nil, false, nil
	}

	qCtx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()

	payload := encodeReceiptQuery(receiptID)
	req := &transport.Message{Type: transport.StreamReceiptQuery, Payload: payload}

	type result struct {
		json  []byte
		found bool
		err   error
	}
	results := make(chan result, len(b.peers))

	var wg sync.WaitGroup
	for _, peer := range b.peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			resp, err := b.caller.Call(qCtx, peer, req)
			if err != nil {
				results <- result{err: err}
				return
			}
			if resp == nil {
				results <- result{}
				return
			}
			parsed := clusterpb.GetRootAsReceiptQueryResponseMsg(resp.Payload, 0)
			if !parsed.Found() {
				results <- result{}
				return
			}
			// Fresh copy of the JSON payload — FlatBuffers byte views
			// alias into `resp.Payload`, which would get reused.
			j := parsed.ReceiptJsonBytes()
			cp := make([]byte, len(j))
			copy(cp, j)
			results <- result{json: cp, found: true}
		}(peer)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var responded, failed int
	for {
		select {
		case <-qCtx.Done():
			b.metrics.OnBroadcastTimeout()
			return nil, false, qCtx.Err()
		case r, ok := <-results:
			if !ok {
				// Channel drained — nobody had it.
				if responded > 0 && responded < len(b.peers) {
					b.metrics.OnBroadcastPartialSuccess(responded, len(b.peers))
				}
				b.metrics.OnBroadcastMiss()
				return nil, false, nil
			}
			if r.err != nil {
				failed++
				continue
			}
			responded++
			if r.found {
				// Report partial success when fewer than len(peers) peers
				// had actually answered by the time of the hit.
				if responded+failed < len(b.peers) {
					b.metrics.OnBroadcastPartialSuccess(responded+failed, len(b.peers))
				}
				b.metrics.OnBroadcastHit()
				return r.json, true, nil
			}
		}
	}
}

// encodeReceiptQuery builds a ReceiptQueryMsg payload for receiptID.
func encodeReceiptQuery(receiptID string) []byte {
	b := flatbuffers.NewBuilder(64)
	idOff := b.CreateString(receiptID)
	clusterpb.ReceiptQueryMsgStart(b)
	clusterpb.ReceiptQueryMsgAddReceiptId(b, idOff)
	b.Finish(clusterpb.ReceiptQueryMsgEnd(b))
	raw := b.FinishedBytes()
	out := make([]byte, len(raw))
	copy(out, raw)
	return out
}

// NewReceiptQueryHandler returns a StreamHandler that answers incoming
// ReceiptQueryMsg requests by consulting the local receipt store (lookup).
// Used by cluster peers to serve the broadcaster's fan-out.
//
// Invalid requests return found=false rather than panicking — the
// transport layer's trust boundary assumes peer auth, but malformed
// payloads should still fail gracefully.
func NewReceiptQueryHandler(lookup ReceiptLookup) func(req *transport.Message) *transport.Message {
	logger := slog.Default().With("component", "receipt-query-handler")
	return func(req *transport.Message) *transport.Message {
		id, err := decodeReceiptQueryID(req.Payload)
		if err != nil || id == "" {
			logger.Warn("receipt-query: invalid payload", "err", err)
			return buildQueryResponseMsg(false, nil)
		}
		data, found := lookup.LookupReceiptJSON(id)
		if !found {
			return buildQueryResponseMsg(false, nil)
		}
		return buildQueryResponseMsg(true, data)
	}
}

func decodeReceiptQueryID(data []byte) (id string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("decode receipt query: invalid flatbuffer: %v", r)
		}
	}()
	msg := clusterpb.GetRootAsReceiptQueryMsg(data, 0)
	return string(msg.ReceiptId()), nil
}

func buildQueryResponseMsg(found bool, receiptJSON []byte) *transport.Message {
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
