package receipt

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeRouteLookup satisfies RouteLookup for tests.
type fakeRouteLookup struct {
	routes map[string]string
}

func (f *fakeRouteLookup) Lookup(id string) (string, bool) {
	n, ok := f.routes[id]
	return n, ok
}

// fakePeerQuerier satisfies PeerQuerier for tests.
type fakePeerQuerier struct {
	singleResp map[string][]byte // peer → receipt JSON
	singleErr  map[string]error
	broadcast  []byte // set to simulate a broadcast hit
	broadcastFound bool
	broadcastErr   error

	singleCalls    []string // peers called
	broadcastCalls int
}

func (f *fakePeerQuerier) QuerySingle(ctx context.Context, peer, id string) ([]byte, bool, error) {
	f.singleCalls = append(f.singleCalls, peer)
	if err, ok := f.singleErr[peer]; ok {
		return nil, false, err
	}
	data, ok := f.singleResp[peer]
	return data, ok, nil
}

func (f *fakePeerQuerier) Query(ctx context.Context, id string) ([]byte, bool, error) {
	f.broadcastCalls++
	return f.broadcast, f.broadcastFound, f.broadcastErr
}

func mustSignedReceipt(t *testing.T, id string, ts time.Time) *HealReceipt {
	t.Helper()
	ks, err := NewKeyStore(Key{ID: "psk-test", Secret: []byte("test-secret")})
	require.NoError(t, err)
	r := &HealReceipt{
		ReceiptID: id,
		Timestamp: ts,
		Object:    ObjectRef{Bucket: "b", Key: "k"},
	}
	require.NoError(t, Sign(r, ks))
	return r
}

func setupAPI(t *testing.T) (*API, *Store, *fakeRouteLookup, *fakePeerQuerier) {
	t.Helper()
	db := openTestDB(t)
	s, err := NewStore(db, StoreOptions{Retention: time.Hour, FlushThreshold: 1, FlushInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	routes := &fakeRouteLookup{routes: map[string]string{}}
	querier := &fakePeerQuerier{singleResp: map[string][]byte{}, singleErr: map[string]error{}}
	api := NewAPI(s, routes, querier)
	return api, s, routes, querier
}

func TestAPI_GetReceipt_LocalHit(t *testing.T) {
	api, s, _, querier := setupAPI(t)

	r := mustSignedReceipt(t, "rcpt-local", time.Now())
	require.NoError(t, s.Put(r))
	require.NoError(t, s.Flush())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/receipts/rcpt-local", nil)
	api.ServeGetReceipt(rec, req, "rcpt-local")

	require.Equal(t, http.StatusOK, rec.Code)
	body, _ := io.ReadAll(rec.Body)
	var decoded HealReceipt
	require.NoError(t, json.Unmarshal(body, &decoded))
	assert.Equal(t, "rcpt-local", decoded.ReceiptID)

	// No peer calls should have happened.
	assert.Empty(t, querier.singleCalls)
	assert.Zero(t, querier.broadcastCalls)
}

func TestAPI_GetReceipt_RouteCacheHit(t *testing.T) {
	api, _, routes, querier := setupAPI(t)

	routes.routes["rcpt-remote"] = "peer-a"
	querier.singleResp["peer-a"] = []byte(`{"receipt_id":"rcpt-remote"}`)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/receipts/rcpt-remote", nil)
	api.ServeGetReceipt(rec, req, "rcpt-remote")

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, []string{"peer-a"}, querier.singleCalls, "should query the cached peer directly")
	assert.Zero(t, querier.broadcastCalls, "no broadcast needed when cache hit succeeds")
}

func TestAPI_GetReceipt_CacheHitButPeerFails_FallsBackToBroadcast(t *testing.T) {
	api, _, routes, querier := setupAPI(t)

	routes.routes["rcpt-x"] = "peer-dead"
	querier.singleErr["peer-dead"] = errors.New("connection refused")
	querier.broadcast = []byte(`{"receipt_id":"rcpt-x","source":"broadcast"}`)
	querier.broadcastFound = true

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/receipts/rcpt-x", nil)
	api.ServeGetReceipt(rec, req, "rcpt-x")

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, []string{"peer-dead"}, querier.singleCalls)
	assert.Equal(t, 1, querier.broadcastCalls)
}

func TestAPI_GetReceipt_NoCache_Broadcasts(t *testing.T) {
	api, _, _, querier := setupAPI(t)

	querier.broadcast = []byte(`{"receipt_id":"rcpt-old"}`)
	querier.broadcastFound = true

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/receipts/rcpt-old", nil)
	api.ServeGetReceipt(rec, req, "rcpt-old")

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Empty(t, querier.singleCalls)
	assert.Equal(t, 1, querier.broadcastCalls)
}

func TestAPI_GetReceipt_NotFoundAnywhere_Returns404(t *testing.T) {
	api, _, _, _ := setupAPI(t)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/receipts/rcpt-missing", nil)
	api.ServeGetReceipt(rec, req, "rcpt-missing")

	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAPI_GetReceipt_BroadcastTimeout_Returns503(t *testing.T) {
	api, _, _, querier := setupAPI(t)

	querier.broadcastErr = context.DeadlineExceeded

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/receipts/rcpt-x", nil)
	api.ServeGetReceipt(rec, req, "rcpt-x")

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Contains(t, rec.Header().Get("X-Heal-Timeout"), "broadcast")
}

func TestAPI_GetReceipt_EmptyID_Returns400(t *testing.T) {
	api, _, _, _ := setupAPI(t)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/receipts/", nil)
	api.ServeGetReceipt(rec, req, "")

	require.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- List endpoint tests ---

func TestAPI_ListReceipts_ReturnsJSONArray(t *testing.T) {
	api, s, _, _ := setupAPI(t)

	base := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		r := mustSignedReceipt(t, uidAPI(i), base.Add(time.Duration(i)*time.Second))
		require.NoError(t, s.Put(r))
	}
	require.NoError(t, s.Flush())

	rec := httptest.NewRecorder()
	url := "/api/receipts?from=" + base.Format(time.RFC3339Nano) +
		"&to=" + base.Add(time.Minute).Format(time.RFC3339Nano)
	req := httptest.NewRequest(http.MethodGet, url, nil)
	api.ServeListReceipts(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var got []HealReceipt
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Len(t, got, 3)
	assert.Equal(t, uidAPI(0), got[0].ReceiptID)
}

func TestAPI_ListReceipts_MissingFromTo_Returns400(t *testing.T) {
	api, _, _, _ := setupAPI(t)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/receipts", nil)
	api.ServeListReceipts(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAPI_ListReceipts_InvalidTimeFormat_Returns400(t *testing.T) {
	api, _, _, _ := setupAPI(t)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/receipts?from=not-a-time&to=also-not", nil)
	api.ServeListReceipts(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAPI_ListReceipts_RespectsLimitParam(t *testing.T) {
	api, s, _, _ := setupAPI(t)

	base := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 10; i++ {
		r := mustSignedReceipt(t, uidAPI(i), base.Add(time.Duration(i)*time.Second))
		require.NoError(t, s.Put(r))
	}
	require.NoError(t, s.Flush())

	rec := httptest.NewRecorder()
	url := "/api/receipts?from=" + base.Format(time.RFC3339Nano) +
		"&to=" + base.Add(time.Minute).Format(time.RFC3339Nano) +
		"&limit=3"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	api.ServeListReceipts(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var got []HealReceipt
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	assert.Len(t, got, 3)
}

func uidAPI(i int) string {
	return "rcpt-api-" + string(rune('0'+i))
}
