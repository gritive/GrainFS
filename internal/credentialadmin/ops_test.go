package credentialadmin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunCreatePostsCredentialAndPrintsSecret(t *testing.T) {
	var got CreateReq
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/v1/credentials", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"id":"pc_123",
			"sa_id":"sa_app",
			"protocol":"nbd",
			"resource":"volume/vol1",
			"mode":"rw",
			"secret":"pcsec_once",
			"connection_hint":{"export_name":"vol1@pcsec_once"}
		}`))
	}))
	t.Cleanup(srv.Close)

	old := NewClientFunc
	NewClientFunc = func(endpoint string) (*Client, error) {
		require.Equal(t, "ignored.sock", endpoint)
		return NewClientForURL(srv.URL), nil
	}
	t.Cleanup(func() { NewClientFunc = old })

	var out strings.Builder
	err := RunCreate(context.Background(), CreateOptions{
		BaseOptions: BaseOptions{Endpoint: "ignored.sock", Stdout: &out},
		SAID:        "sa_app",
		Protocol:    "nbd",
		Resource:    "volume/vol1",
		Mode:        "rw",
	})
	require.NoError(t, err)
	require.Equal(t, CreateReq{SAID: "sa_app", Protocol: "nbd", Resource: "volume/vol1", Mode: "rw"}, got)
	require.Contains(t, out.String(), "id:              pc_123")
	require.Contains(t, out.String(), "secret:          pcsec_once")
	require.Contains(t, out.String(), "connection_hint: export_name=vol1@pcsec_once")
}

func TestClientListSendsFilters(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "/v1/credentials", r.URL.Path)
		require.Equal(t, "sa_app", r.URL.Query().Get("sa_id"))
		require.Equal(t, "nbd", r.URL.Query().Get("protocol"))
		require.Equal(t, "volume/vol1", r.URL.Query().Get("resource"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"credentials":[{"id":"pc_123","sa_id":"sa_app","protocol":"nbd","resource":"volume/vol1","mode":"rw"}]}`))
	}))
	t.Cleanup(srv.Close)

	resp, err := NewClientForURL(srv.URL).ListFiltered(context.Background(), "sa_app", "nbd", "volume/vol1")
	require.NoError(t, err)
	require.Len(t, resp.Credentials, 1)
	require.Equal(t, "pc_123", resp.Credentials[0].ID)
}

func TestRunCreateRequiresFields(t *testing.T) {
	err := RunCreate(context.Background(), CreateOptions{})
	require.ErrorContains(t, err, "--sa required")
}
