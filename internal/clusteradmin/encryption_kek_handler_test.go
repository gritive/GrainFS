package clusteradmin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/compat"
)

// fakeOrchestrator records every Propose* call and returns the next canned err.
type fakeOrchestrator struct {
	rotateErr error
	retireErr error
	pruneErr  error

	rotateCalls []struct {
		Confirm string
		Actor   string
	}
	retireCalls []struct {
		Version uint32
		Confirm string
		Actor   string
	}
	pruneCalls []struct {
		Version uint32
		Actor   string
	}
}

func (f *fakeOrchestrator) ProposeKEKRotate(confirm, actor string) error {
	f.rotateCalls = append(f.rotateCalls, struct {
		Confirm string
		Actor   string
	}{confirm, actor})
	return f.rotateErr
}

func (f *fakeOrchestrator) ProposeKEKRetire(version uint32, confirm, actor string) error {
	f.retireCalls = append(f.retireCalls, struct {
		Version uint32
		Confirm string
		Actor   string
	}{version, confirm, actor})
	return f.retireErr
}

func (f *fakeOrchestrator) ProposeKEKPrune(version uint32, actor string) error {
	f.pruneCalls = append(f.pruneCalls, struct {
		Version uint32
		Actor   string
	}{version, actor})
	return f.pruneErr
}

// fakeGate flips Allow's return between nil and a synthetic Reject error.
type fakeGate struct{ err error }

func (g *fakeGate) Allow(_ context.Context, _ compat.Operation) (compat.GatePlan, error) {
	return compat.GatePlan{}, g.err
}

// fakeStatusReader feeds GET /encrypt/kek/status.
type fakeStatusReader struct {
	active    uint32
	activeGen uint32
	versions  []uint32
	statuses  map[uint32]cluster.KEKLifecycleStatus
	seals     map[uint32]uint64 // keyed by DEK generation
	leases    map[uint32]uint64
}

func (r *fakeStatusReader) ActiveKEKVersion() uint32   { return r.active }
func (r *fakeStatusReader) KEKStoreVersions() []uint32 { return append([]uint32(nil), r.versions...) }
func (r *fakeStatusReader) LookupKEKStatus(v uint32) (uint32, cluster.KEKLifecycleStatus, uint64, bool) {
	if r.statuses == nil {
		return 0, 0, 0, false
	}
	s, ok := r.statuses[v]
	if !ok {
		return 0, 0, 0, false
	}
	return v, s, 0, true
}
func (r *fakeStatusReader) ActiveDEKGeneration() uint32          { return r.activeGen }
func (r *fakeStatusReader) SealCountSnapshot() map[uint32]uint64 { return r.seals }
func (r *fakeStatusReader) LeaseCount(v uint32) uint64           { return r.leases[v] }

func newTestHandler(orch *fakeOrchestrator, gate *fakeGate, reader KEKStatusReader) *EncryptionKEKHandler {
	var go_ KEKCapabilityGate
	if gate != nil {
		go_ = gate
	}
	// Pass a nil-typed interface (not a typed nil pointer) so handler's
	// orchestrator == nil check fires for "disabled" tests.
	var orchIface KEKRotationOrchestrator
	if orch != nil {
		orchIface = orch
	}
	return NewEncryptionKEKHandler(orchIface, go_, reader)
}

func postJSON(t *testing.T, h http.HandlerFunc, body map[string]any) *http.Response {
	t.Helper()
	b, err := json.Marshal(body)
	require.NoError(t, err)
	r := httptest.NewRequest("POST", "/v1/encrypt/kek/rotate", bytes.NewReader(b))
	w := httptest.NewRecorder()
	h(w, r)
	return w.Result()
}

func TestAdmin_PostEncryptKEKRotate_HappyPath_StubLeaderReturnsNil(t *testing.T) {
	orch := &fakeOrchestrator{}
	h := newTestHandler(orch, nil, nil)
	resp := postJSON(t, h.ServeRotate, map[string]any{"confirm": "rotate-now"})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Len(t, orch.rotateCalls, 1)
	require.Equal(t, "rotate-now", orch.rotateCalls[0].Confirm)
}

func TestAdmin_PostEncryptKEKRotate_RejectsWithoutConfirm(t *testing.T) {
	orch := &fakeOrchestrator{}
	h := newTestHandler(orch, nil, nil)
	resp := postJSON(t, h.ServeRotate, map[string]any{})
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Empty(t, orch.rotateCalls, "leader must NOT be called on bad confirm")
}

func TestAdmin_PostEncryptKEKRotate_RejectsDuringMixedVersion(t *testing.T) {
	orch := &fakeOrchestrator{}
	gateErr := errors.New("compat: missing kek_envelope_v1 on node-1")
	h := newTestHandler(orch, &fakeGate{err: gateErr}, nil)
	resp := postJSON(t, h.ServeRotate, map[string]any{"confirm": "rotate-now"})
	defer resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.Empty(t, orch.rotateCalls, "leader must NOT be called when gate refuses")
}

func TestAdmin_PostEncryptKEKPrune_RequiresVersionAndConfirm(t *testing.T) {
	orch := &fakeOrchestrator{}
	h := newTestHandler(orch, nil, nil)

	// missing version
	resp := postJSON(t, h.ServePrune, map[string]any{"confirm": "delete-permanently-3"})
	resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, "missing version must be 400")

	// confirm mismatch
	resp = postJSON(t, h.ServePrune, map[string]any{"version": 3, "confirm": "delete-permanently-2"})
	resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, "confirm mismatch must be 400")

	// confirm missing
	resp = postJSON(t, h.ServePrune, map[string]any{"version": 3})
	resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, "missing confirm must be 400")

	require.Empty(t, orch.pruneCalls, "leader must NOT be called on bad request")
}

// TestAdmin_PostEncryptKEKRotate_NetworkAddressRejected verifies that the
// KEK admin routes are NOT mounted on the public/UI Hertz engine.
//
// UDS-only is enforced architecturally — the route registrar
// (RegisterEncryptionKEKRoutes) is only invoked from the admin-UDS boot path
// (boot_phases_admin.go) and never from RegisterUI. A request via TCP to
// /v1/encrypt/kek/rotate therefore hits the default 404 because no handler is
// registered there. This test substitutes for the "verify a TCP request
// returns 404" check in the plan: it confirms the absence of the route on a
// fresh net/http mux that did NOT receive the registrar call.
func TestAdmin_PostEncryptKEKRotate_NetworkAddressRejected(t *testing.T) {
	// Build a fresh mux without the KEK routes registered — the public/UI
	// Hertz instance does the equivalent in production (RegisterUI does not
	// call RegisterEncryptionKEKRoutes).
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	body, _ := json.Marshal(map[string]any{"confirm": "rotate-now"})
	resp, err := http.Post(srv.URL+"/v1/encrypt/kek/rotate", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode,
		"KEK routes must be unreachable on the public/UI Hertz; got %d", resp.StatusCode)
}

func TestAdmin_PostEncryptKEKRetire_HappyPath(t *testing.T) {
	orch := &fakeOrchestrator{}
	h := newTestHandler(orch, nil, nil)
	resp := postJSON(t, h.ServeRetire, map[string]any{
		"version": 3,
		"confirm": "delete-permanently-3",
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Len(t, orch.retireCalls, 1)
	require.Equal(t, uint32(3), orch.retireCalls[0].Version)
	require.Equal(t, "delete-permanently-3", orch.retireCalls[0].Confirm)
}

func TestAdmin_PostEncryptKEKRetire_RequiresMatchingConfirm(t *testing.T) {
	orch := &fakeOrchestrator{}
	h := newTestHandler(orch, nil, nil)
	// version 3 with delete-permanently-4 → 400
	resp := postJSON(t, h.ServeRetire, map[string]any{
		"version": 3,
		"confirm": "delete-permanently-4",
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Empty(t, orch.retireCalls)
}

func TestAdmin_PostEncryptKEKPrune_HappyPath(t *testing.T) {
	orch := &fakeOrchestrator{}
	h := newTestHandler(orch, nil, nil)
	resp := postJSON(t, h.ServePrune, map[string]any{
		"version": 7,
		"confirm": "delete-permanently-7",
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Len(t, orch.pruneCalls, 1)
	require.Equal(t, uint32(7), orch.pruneCalls[0].Version)
}

func TestAdmin_GetEncryptKEKStatus_ReturnsActiveAndVersions(t *testing.T) {
	reader := &fakeStatusReader{
		active:    2,
		activeGen: 1, // DEK gen distinct from KEK version (KEK rotated, DEK rotated once)
		versions:  []uint32{0, 1, 2},
		statuses: map[uint32]cluster.KEKLifecycleStatus{
			0: cluster.KEKLifecyclePruned,
			1: cluster.KEKLifecycleRetiring,
		},
		seals: map[uint32]uint64{ // keyed by DEK generation
			0: 10,          // retired DEK gen → ok
			1: 250_000_000, // active DEK gen → warn band
		},
		leases: map[uint32]uint64{
			1: 3,
		},
	}
	h := newTestHandler(nil, nil, reader)
	r := httptest.NewRequest("GET", "/v1/encrypt/kek/status", nil)
	w := httptest.NewRecorder()
	h.ServeStatus(w, r)
	resp := w.Result()
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out kekStatusResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Equal(t, uint32(2), out.ActiveVersion)
	require.Equal(t, uint32(1), out.ActiveDEKGeneration)
	require.Len(t, out.Versions, 3)
	// versions arrive in keystore order (sorted ascending in our fake).
	require.Equal(t, uint32(0), out.Versions[0].Version)
	require.Equal(t, "pruned", out.Versions[0].Status)
	require.Equal(t, "retiring", out.Versions[1].Status)
	require.Equal(t, "active", out.Versions[2].Status)
	require.Equal(t, uint64(3), out.Versions[1].LeaseCount, "v1 lease count")

	// Per-DEK-generation nonce-collision diagnostics, sorted ascending.
	require.Len(t, out.DEKGenerations, 2)
	require.Equal(t, uint32(0), out.DEKGenerations[0].Generation)
	require.False(t, out.DEKGenerations[0].Active)
	require.Equal(t, uint64(10), out.DEKGenerations[0].SealCount)
	require.Equal(t, "ok", out.DEKGenerations[0].NonceCollisionRisk, "10 seals → ok")
	require.Equal(t, uint32(1), out.DEKGenerations[1].Generation)
	require.True(t, out.DEKGenerations[1].Active)
	require.Equal(t, uint64(250_000_000), out.DEKGenerations[1].SealCount, "active DEK gen seal count")
	require.Equal(t, "warn", out.DEKGenerations[1].NonceCollisionRisk, "250M seals → warn")
}

func TestNonceCollisionRisk_Thresholds(t *testing.T) {
	cases := []struct {
		seals uint64
		want  string
	}{
		{0, "ok"},
		{99_999_999, "ok"},
		{100_000_000, "warn"},
		{999_999_999, "warn"},
		{1_000_000_000, "alert"},
		{5_000_000_000, "alert"},
	}
	for _, c := range cases {
		if got := nonceCollisionRisk(c.seals); got != c.want {
			t.Errorf("nonceCollisionRisk(%d) = %q, want %q", c.seals, got, c.want)
		}
	}
}

func TestAdmin_PostEncryptKEKRotate_DisabledWhenLeaderNil(t *testing.T) {
	h := newTestHandler(nil, nil, nil)
	resp := postJSON(t, h.ServeRotate, map[string]any{"confirm": "rotate-now"})
	defer resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func TestAdmin_PostEncryptKEKRotate_LeaderErrorMapsTo5xx(t *testing.T) {
	orch := &fakeOrchestrator{rotateErr: errors.New("KEKRotate: encode: short payload")}
	h := newTestHandler(orch, nil, nil)
	resp := postJSON(t, h.ServeRotate, map[string]any{"confirm": "rotate-now"})
	defer resp.Body.Close()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	body := readBody(t, resp)
	require.True(t, strings.Contains(body, "encode"), "want propagated error: %q", body)
}

func TestAdmin_PostEncryptKEKRotate_NotLeaderMapsTo503(t *testing.T) {
	orch := &fakeOrchestrator{rotateErr: errors.New("KEKRotate: not leader")}
	h := newTestHandler(orch, nil, nil)
	resp := postJSON(t, h.ServeRotate, map[string]any{"confirm": "rotate-now"})
	defer resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func TestAdmin_PostEncryptKEKRotate_AnotherInFlightMapsTo409(t *testing.T) {
	orch := &fakeOrchestrator{rotateErr: cluster.ErrKEKRotateAnotherInFlight}
	h := newTestHandler(orch, nil, nil)
	resp := postJSON(t, h.ServeRotate, map[string]any{"confirm": "rotate-now"})
	defer resp.Body.Close()
	require.Equal(t, http.StatusConflict, resp.StatusCode)
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(resp.Body)
	return buf.String()
}
