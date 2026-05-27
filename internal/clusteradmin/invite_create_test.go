package clusteradmin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// inviteStubHandler records the request and replies with the given status +
// JSON body. Mirrors joinStubHandler.
func inviteStubHandler(captured *[]byte, statusCode int, response any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		*captured = body
		w.Header().Set("Content-Type", "application/json")
		code := statusCode
		if code == 0 {
			code = http.StatusOK
		}
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(response)
	}
}

func TestRunInviteCreate_PrintsBundle(t *testing.T) {
	var captured []byte
	srv := httptest.NewServer(inviteStubHandler(&captured, http.StatusOK, map[string]string{
		"bundle":    "BUNDLE-TOKEN-XYZ",
		"invite_id": "01890000-0000-7000-8000-000000000000",
	}))
	defer srv.Close()

	var out bytes.Buffer
	err := RunInviteCreate(context.Background(), InviteCreateOptions{
		Endpoint: srv.URL,
		TTL:      2 * time.Hour,
		Out:      &out,
	})
	require.NoError(t, err)

	// Wire shape: TTL forwarded as ttl_nanos.
	assert.JSONEq(t, `{"ttl_nanos":7200000000000}`, string(captured))
	// Bundle + invite id surfaced to the operator.
	assert.Contains(t, out.String(), "BUNDLE-TOKEN-XYZ")
	assert.Contains(t, out.String(), "01890000-0000-7000-8000-000000000000")
	assert.Contains(t, out.String(), "GRAINFS_INVITE_BUNDLE")
}

func TestInviteCreate_NotLeader_TypedError(t *testing.T) {
	var captured []byte
	srv := httptest.NewServer(inviteStubHandler(&captured, http.StatusConflict, map[string]string{
		"error":     "not leader: run on the meta-raft leader",
		"leader_id": "node-1",
	}))
	defer srv.Close()

	_, err := NewClient(srv.URL).InviteCreate(context.Background(), time.Hour)
	require.Error(t, err)
	var notLeader *InviteNotLeaderError
	require.True(t, errors.As(err, &notLeader), "expected *InviteNotLeaderError, got %T", err)
	assert.Equal(t, "node-1", notLeader.LeaderID)
	assert.True(t, strings.Contains(notLeader.Error(), "node-1"))
}
