package adminapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
)

func TestClusterConfigHandler_Get_Empty(t *testing.T) {
	fsm := cluster.NewMetaFSM()
	h := NewClusterConfigHandler(fsm, nil)
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
	h := NewClusterConfigHandler(fsm, nil)
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
