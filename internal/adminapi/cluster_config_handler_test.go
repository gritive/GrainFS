package adminapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
)

// fakeProposer simulates leader-side raft by applying the encoded patch
// directly to the local FSM (i.e. bypassing the raft log). Matches the
// production semantics: ProposeClusterConfigPatch returns nil only after the
// patch is applied to the FSM.
type fakeProposer struct {
	fsm *cluster.MetaFSM
}

func (p *fakeProposer) ProposeClusterConfigPatch(patch cluster.ClusterConfigPatch) error {
	return p.fsm.ApplyClusterConfigPatchForTest(patch)
}

func newTestEncryptor(t *testing.T) *encrypt.Encryptor {
	t.Helper()
	key := bytes.Repeat([]byte{0xab}, 32)
	enc, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)
	return enc
}

func TestClusterConfigHandler_Get_Empty(t *testing.T) {
	fsm := cluster.NewMetaFSM()
	h := NewClusterConfigHandler(fsm, nil, nil)
	srv := httptest.NewServer(http.HandlerFunc(h.ServeHTTP))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/v1/cluster/config")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out ClusterConfigResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Equal(t, uint64(0), out.Rev)
	// All keys appear with source=default
	require.NotEmpty(t, out.Effective)
	for _, key := range cluster.AllConfigKeys() {
		require.Equal(t, "default", out.Source[key], "key=%s", key)
	}
}

func TestClusterConfigHandler_Get_AfterPatch(t *testing.T) {
	fsm := cluster.NewMetaFSM()
	require.NoError(t, fsm.ApplyClusterConfigPatchForTest(cluster.ClusterConfigPatch{
		BalancerImbalanceTriggerPct: ptrFloat(28.0),
	}))
	h := NewClusterConfigHandler(fsm, nil, nil)
	rec := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/v1/cluster/config", nil)
	h.ServeHTTP(rec, r)
	require.Equal(t, 200, rec.Code)

	var out ClusterConfigResponse
	require.NoError(t, json.NewDecoder(strings.NewReader(rec.Body.String())).Decode(&out))
	require.Equal(t, "explicit", out.Source["balancer-imbalance-trigger-pct"])
	require.Equal(t, 28.0, out.Effective["balancer-imbalance-trigger-pct"])
	require.Equal(t, "default", out.Source["alert-webhook"])
}

func ptrFloat(v float64) *float64 { return &v }

func TestClusterConfigHandler_Patch_SetsTrigger(t *testing.T) {
	fsm := cluster.NewMetaFSM()
	fsm.SetEncryptor(newTestEncryptor(t))
	prop := &fakeProposer{fsm: fsm}
	h := NewClusterConfigHandler(fsm, prop, fsm.Encryptor())
	body := `{"balancer-imbalance-trigger-pct": 27.5}`
	req := httptest.NewRequest("PATCH", "/v1/cluster/config", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, 200, rec.Code, "body=%s", rec.Body.String())
	require.InDelta(t, 27.5, fsm.ClusterConfig().BalancerImbalanceTriggerPct(), 0.0001)
}

func TestClusterConfigHandler_Patch_ValidationError(t *testing.T) {
	fsm := cluster.NewMetaFSM()
	fsm.SetEncryptor(newTestEncryptor(t))
	prop := &fakeProposer{fsm: fsm}
	h := NewClusterConfigHandler(fsm, prop, fsm.Encryptor())
	body := `{"balancer-imbalance-trigger-pct": 200}`
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("PATCH", "/v1/cluster/config", strings.NewReader(body)))
	require.Equal(t, 400, rec.Code)
	require.Contains(t, rec.Body.String(), "balancer-imbalance-trigger-pct")
}

func TestClusterConfigHandler_Patch_CAS_Mismatch(t *testing.T) {
	fsm := cluster.NewMetaFSM()
	fsm.SetEncryptor(newTestEncryptor(t))
	require.NoError(t, fsm.ApplyClusterConfigPatchForTest(cluster.ClusterConfigPatch{
		BalancerImbalanceTriggerPct: ptrFloat(25.0),
	}))
	// fsm rev == 1
	prop := &fakeProposer{fsm: fsm}
	h := NewClusterConfigHandler(fsm, prop, fsm.Encryptor())
	req := httptest.NewRequest("PATCH", "/v1/cluster/config", strings.NewReader(`{"balancer-imbalance-trigger-pct":30}`))
	req.Header.Set("If-Match-Rev", "7")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, 409, rec.Code)
}

func TestClusterConfigHandler_Patch_SecretNoEncryption_Forbidden(t *testing.T) {
	fsm := cluster.NewMetaFSM() // no encryptor set
	prop := &fakeProposer{fsm: fsm}
	h := NewClusterConfigHandler(fsm, prop, nil) // no encryptor in handler either
	body := `{"alert-webhook":"https://hooks.example/x","alert-webhook-secret":"shh"}`
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("PATCH", "/v1/cluster/config", strings.NewReader(body)))
	require.Equal(t, 403, rec.Code)
	require.Contains(t, rec.Body.String(), "encryption")
}

func TestClusterConfigHandler_Patch_Reset(t *testing.T) {
	fsm := cluster.NewMetaFSM()
	fsm.SetEncryptor(newTestEncryptor(t))
	require.NoError(t, fsm.ApplyClusterConfigPatchForTest(cluster.ClusterConfigPatch{
		BalancerImbalanceTriggerPct: ptrFloat(35.0),
	}))
	prop := &fakeProposer{fsm: fsm}
	h := NewClusterConfigHandler(fsm, prop, fsm.Encryptor())
	body := `{"reset_keys":["balancer-imbalance-trigger-pct"]}`
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("PATCH", "/v1/cluster/config", strings.NewReader(body)))
	require.Equal(t, 200, rec.Code, "body=%s", rec.Body.String())
	require.InDelta(t, cluster.DefaultClusterBalancerImbalanceTriggerPct, fsm.ClusterConfig().BalancerImbalanceTriggerPct(), 0.0001)
}
