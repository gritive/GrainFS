package cluster

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

// fakeEvidenceSource is a test CapabilityEvidenceSource.
type fakeEvidenceSource struct {
	caps map[string]bool
}

func (f *fakeEvidenceSource) CapabilityEvidence(nodeID string, now time.Time) compat.Evidence {
	return compat.Evidence{
		NodeID:       compat.NodeID(nodeID),
		Capabilities: f.caps,
		LastSeen:     now,
		Ready:        true,
	}
}

// Ensure fakeEvidenceSource satisfies the interface.
var _ CapabilityEvidenceSource = (*fakeEvidenceSource)(nil)

// newCapProbeKEKStore returns a KEKStore loaded with a single test KEK at version 0.
// The key is filled with keyByte repeated KEKSize times.
func newCapProbeKEKStore(t *testing.T, keyByte byte) *encrypt.KEKStore {
	t.Helper()
	s := encrypt.NewKEKStore()
	kek := bytes.Repeat([]byte{keyByte}, encrypt.KEKSize)
	if err := s.Add(0, kek); err != nil {
		t.Fatalf("newCapProbeKEKStore Add: %v", err)
	}
	return s
}

var testClusterID = bytes.Repeat([]byte{0xAB}, 16)

// makeTestDialer wires a CapabilityProbeHandler as a capabilityProbeDialer.
// No QUIC is needed — the handler is called in-process.
func makeTestDialer(handler *CapabilityProbeHandler) capabilityProbeDialer {
	return func(ctx context.Context, peer string, payload []byte) ([]byte, error) {
		msg := &transport.Message{
			Type:    transport.StreamCapabilityProbe,
			Payload: payload,
		}
		resp := handler.Handle(msg)
		if resp.Status != transport.StatusOK {
			return nil, errors.New(string(resp.Payload))
		}
		return resp.Payload, nil
	}
}

// TestCapabilityDirectRPC_SignedAssertion_RoundTrip verifies that a client can
// request capabilities from a server, the server signs the assertion with the
// cluster KEK, and the client verifies the signature successfully.
func TestCapabilityDirectRPC_SignedAssertion_RoundTrip(t *testing.T) {
	kekStore := newCapProbeKEKStore(t, 0x42)
	evidenceSource := &fakeEvidenceSource{
		caps: map[string]bool{
			compat.CapabilityKEKEnvelopeV1: true,
		},
	}

	handler := NewCapabilityProbeHandler("node-A", "0.0.344.0", testClusterID, kekStore, evidenceSource)
	dialer := makeTestDialer(handler)

	resp, err := GetCapabilities(context.Background(), "node-A", testClusterID, kekStore, dialer)
	if err != nil {
		t.Fatalf("GetCapabilities: %v", err)
	}
	if resp.ServerID != "node-A" {
		t.Errorf("ServerID = %q, want %q", resp.ServerID, "node-A")
	}
	if resp.BinaryVersion != "0.0.344.0" {
		t.Errorf("BinaryVersion = %q, want %q", resp.BinaryVersion, "0.0.344.0")
	}
	found := false
	for _, c := range resp.Capabilities {
		if c == compat.CapabilityKEKEnvelopeV1 {
			found = true
		}
	}
	if !found {
		t.Errorf("kek_envelope_v1 not present in capabilities: %v", resp.Capabilities)
	}
}

// TestCapabilityDirectRPC_AssertionForWrongServer_Rejects verifies that if a
// server returns a response where the SignedAssertion was signed for "node-B"
// but the ServerID field says "node-A", the client rejects with a mismatch error.
func TestCapabilityDirectRPC_AssertionForWrongServer_Rejects(t *testing.T) {
	kekStore := newCapProbeKEKStore(t, 0x55)
	evidenceSource := &fakeEvidenceSource{
		caps: map[string]bool{compat.CapabilityKEKEnvelopeV1: true},
	}

	// A handler that legitimately serves node-B requests.
	handlerB := NewCapabilityProbeHandler("node-B", "0.0.344.0", testClusterID, kekStore, evidenceSource)

	// Rogue dialer: gets a valid signed assertion for node-B, then lies and
	// returns ServerID="node-A" while keeping the node-B signature.
	rogueDialer := func(ctx context.Context, peer string, payload []byte) ([]byte, error) {
		var nodeBNonce [32]byte
		for i := range nodeBNonce {
			nodeBNonce[i] = byte(i)
		}
		internalReq := encodeCapProbeRequest(capabilityProbeRequest{
			ExpectedServerID: "node-B",
			Nonce:            nodeBNonce,
		})
		msg := &transport.Message{
			Type:    transport.StreamCapabilityProbe,
			Payload: internalReq,
		}
		nodeBRespMsg := handlerB.Handle(msg)
		if nodeBRespMsg.Status != transport.StatusOK {
			return nil, errors.New(string(nodeBRespMsg.Payload))
		}
		decoded, err := decodeCapProbeResponse(nodeBRespMsg.Payload)
		if err != nil {
			return nil, err
		}
		// Lie: replace ServerID with node-A while keeping node-B's signed assertion.
		decoded.ServerID = "node-A"
		return encodeCapProbeResponse(decoded), nil
	}

	_, err := GetCapabilities(context.Background(), "node-A", testClusterID, kekStore, rogueDialer)
	if err == nil {
		t.Fatal("expected error when assertion was signed for node-B but response claims node-A, got nil")
	}
	if !capTestContains(err.Error(), "assertion content mismatch") && !capTestContains(err.Error(), "assertion signature invalid") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestCapabilityDirectRPC_ReplayedNonce_Rejects verifies that a replay of a
// previous assertion (different client nonce, same signed bytes) is rejected.
// The first call succeeds; the replay dialer returns the captured first response
// unchanged for the second call (which has a fresh, different nonce).
func TestCapabilityDirectRPC_ReplayedNonce_Rejects(t *testing.T) {
	kekStore := newCapProbeKEKStore(t, 0x77)
	evidenceSource := &fakeEvidenceSource{
		caps: map[string]bool{compat.CapabilityKEKEnvelopeV1: true},
	}

	handler := NewCapabilityProbeHandler("node-C", "0.0.344.0", testClusterID, kekStore, evidenceSource)

	var capturedResp []byte

	// First call: record the honest response.
	recordingDialer := makeTestDialer(handler)
	wrappedDialer := func(ctx context.Context, peer string, payload []byte) ([]byte, error) {
		resp, err := recordingDialer(ctx, peer, payload)
		if err == nil && capturedResp == nil {
			capturedResp = append([]byte(nil), resp...)
		}
		return resp, err
	}

	// First call must succeed.
	_, err := GetCapabilities(context.Background(), "node-C", testClusterID, kekStore, wrappedDialer)
	if err != nil {
		t.Fatalf("first call failed: %v", err)
	}

	// Replay dialer: always returns the captured first response (stale nonce).
	replayDialer := func(ctx context.Context, peer string, payload []byte) ([]byte, error) {
		return capturedResp, nil
	}

	// Second call with a fresh nonce but replayed assertion must fail.
	_, err = GetCapabilities(context.Background(), "node-C", testClusterID, kekStore, replayDialer)
	if err == nil {
		t.Fatal("expected error on replayed assertion, got nil")
	}
	if !capTestContains(err.Error(), "assertion content mismatch") && !capTestContains(err.Error(), "assertion signature invalid") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestCapabilityGateAllow_CacheInvalidatesOnKEKRotation verifies that the
// Allow cache key includes active_kek_version so a KEK rotation invalidates
// the cached result and triggers a new direct probe.
func TestCapabilityGateAllow_CacheInvalidatesOnKEKRotation(t *testing.T) {
	kekStore := newCapProbeKEKStore(t, 0x33)
	evidenceSource := &fakeEvidenceSource{
		caps: map[string]bool{compat.CapabilityKEKEnvelopeV1: true},
	}

	handler := NewCapabilityProbeHandler("node-D", "0.0.344.0", testClusterID, kekStore, evidenceSource)

	// Instrument the dialer to count actual probe calls.
	var probeCount int
	countingDialer := func(ctx context.Context, peer string, payload []byte) ([]byte, error) {
		probeCount++
		return makeTestDialer(handler)(ctx, peer, payload)
	}

	gate := NewCapabilityGate(compat.DefaultRegistry, 30*time.Second)
	cfg := raft.Configuration{Servers: []raft.Server{
		{ID: "node-D", Suffrage: raft.Voter},
	}}
	gate.SetMetaRaftSnapshot(1, cfg)
	gate.WithDirectProbe(testClusterID, kekStore, countingDialer)

	// First Allow — cache miss, must probe.
	plan1, err1 := gate.Allow(context.Background(), compat.OperationKEKRotate)
	if err1 != nil {
		t.Fatalf("first Allow: %v (plan=%+v)", err1, plan1)
	}
	if !plan1.Allowed() {
		t.Fatalf("first Allow should be allowed, plan=%+v", plan1)
	}
	if probeCount != 1 {
		t.Errorf("after first Allow: probeCount = %d, want 1", probeCount)
	}

	// Second Allow — same cache key (op, configID, kekVer=0), must hit cache.
	_, err2 := gate.Allow(context.Background(), compat.OperationKEKRotate)
	if err2 != nil {
		t.Fatalf("second Allow (cached): %v", err2)
	}
	if probeCount != 1 {
		t.Errorf("after cached Allow: probeCount = %d, want 1 (no new probe on cache hit)", probeCount)
	}

	// Rotate the KEK: add v1 — this changes ActiveVersion() from 0 to 1.
	kek2 := bytes.Repeat([]byte{0x44}, encrypt.KEKSize)
	if err := kekStore.Add(1, kek2); err != nil {
		t.Fatalf("Add KEK v1: %v", err)
	}

	// Allow after KEK rotation — cache key changes (activeKEKVer=1), must probe again.
	plan3, err3 := gate.Allow(context.Background(), compat.OperationKEKRotate)
	if err3 != nil {
		t.Fatalf("Allow after KEK rotation: %v (plan=%+v)", err3, plan3)
	}
	if !plan3.Allowed() {
		t.Fatalf("Allow after KEK rotation should be allowed")
	}
	if probeCount != 2 {
		t.Errorf("after KEK rotation: probeCount = %d, want 2 (cache must have missed)", probeCount)
	}
}

// TestCapabilityGate_Allow_ConcurrentCacheMiss_SingleFanout verifies that
// multiple goroutines racing on the same cache miss fire the probe fan-out
// exactly once, not once per goroutine.
func TestCapabilityGate_Allow_ConcurrentCacheMiss_SingleFanout(t *testing.T) {
	kekStore := newCapProbeKEKStore(t, 0x11)
	evidenceSource := &fakeEvidenceSource{
		caps: map[string]bool{compat.CapabilityKEKEnvelopeV1: true},
	}
	handler := NewCapabilityProbeHandler("node-SF", "0.0.344.0", testClusterID, kekStore, evidenceSource)

	var probeCount atomic.Int32
	// start gate is used to hold all goroutines at the same point before Allow is
	// called, maximising the chance of a true concurrent cache miss.
	start := make(chan struct{})
	countingDialer := func(ctx context.Context, peer string, payload []byte) ([]byte, error) {
		probeCount.Add(1)
		return makeTestDialer(handler)(ctx, peer, payload)
	}

	gate := NewCapabilityGate(compat.DefaultRegistry, 30*time.Second)
	cfg := raft.Configuration{Servers: []raft.Server{
		{ID: "node-SF", Suffrage: raft.Voter},
	}}
	gate.SetMetaRaftSnapshot(1, cfg)
	gate.WithDirectProbe(testClusterID, kekStore, countingDialer)

	const concurrency = 8
	var wg sync.WaitGroup
	errs := make([]error, concurrency)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start // wait until all goroutines are ready
			_, errs[idx] = gate.Allow(context.Background(), compat.OperationKEKRotate)
		}(i)
	}
	close(start) // release all goroutines simultaneously
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: Allow returned error: %v", i, err)
		}
	}
	if got := probeCount.Load(); got != 1 {
		t.Errorf("probe fan-out = %d, want 1 (singleflight not coalescing concurrent misses)", got)
	}
}

// capTestContains returns true if s contains sub.
func capTestContains(s, sub string) bool {
	if len(sub) == 0 {
		return true
	}
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
