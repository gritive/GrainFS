package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestVerifyPerVersionCutoverHandler_NotWired proves the handler returns 503
// when no VerifyPerVersionCutoverFunc is injected (single-node / cluster mode
// not configured).
func TestVerifyPerVersionCutoverHandler_NotWired(t *testing.T) {
	sock := udsTestSocket(t)
	cli := startUDSAdminTestServerWithSrv(t, sock, &Server{})

	resp, err := cli.Get("http://unix/v1/cluster/verify-per-version-cutover")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

// TestVerifyPerVersionCutoverHandler_AllReady proves the handler returns 200
// with expected counts when all versions are complete.
func TestVerifyPerVersionCutoverHandler_AllReady(t *testing.T) {
	sock := udsTestSocket(t)
	called := false
	s := &Server{
		verifyPerVersionCutoverFn: func(_ context.Context, bucket string) (PerVersionCutoverReadiness, error) {
			called = true
			require.Equal(t, "", bucket, "no bucket filter expected")
			return PerVersionCutoverReadiness{Complete: 42, Excluded: 3}, nil
		},
	}
	cli := startUDSAdminTestServerWithSrv(t, sock, s)

	resp, err := cli.Get("http://unix/v1/cluster/verify-per-version-cutover")
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	require.True(t, called)

	var out PerVersionCutoverReadiness
	require.NoError(t, json.Unmarshal(body, &out))
	require.Equal(t, 42, out.Complete)
	require.Equal(t, 3, out.Excluded)
	require.Equal(t, 0, out.Gaps)
	require.Equal(t, 0, out.Stuck)
	require.Equal(t, 0, out.Unknown)
}

// TestVerifyPerVersionCutoverHandler_WithGaps proves the handler returns 200
// with gap counts and gap_refs populated.
func TestVerifyPerVersionCutoverHandler_WithGaps(t *testing.T) {
	sock := udsTestSocket(t)
	s := &Server{
		verifyPerVersionCutoverFn: func(_ context.Context, bucket string) (PerVersionCutoverReadiness, error) {
			return PerVersionCutoverReadiness{
				Complete: 10,
				Gaps:     2,
				GapRefs:  []string{"bkt/key1@vid1", "bkt/key2@vid2"},
			}, nil
		},
	}
	cli := startUDSAdminTestServerWithSrv(t, sock, s)

	resp, err := cli.Get("http://unix/v1/cluster/verify-per-version-cutover")
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out PerVersionCutoverReadiness
	require.NoError(t, json.Unmarshal(body, &out))
	require.Equal(t, 2, out.Gaps)
	require.Equal(t, []string{"bkt/key1@vid1", "bkt/key2@vid2"}, out.GapRefs)
}

// TestVerifyPerVersionCutoverHandler_BucketFilter proves ?bucket= is forwarded
// to the injected func.
func TestVerifyPerVersionCutoverHandler_BucketFilter(t *testing.T) {
	sock := udsTestSocket(t)
	var gotBucket string
	s := &Server{
		verifyPerVersionCutoverFn: func(_ context.Context, bucket string) (PerVersionCutoverReadiness, error) {
			gotBucket = bucket
			return PerVersionCutoverReadiness{Complete: 5}, nil
		},
	}
	cli := startUDSAdminTestServerWithSrv(t, sock, s)

	resp, err := cli.Get("http://unix/v1/cluster/verify-per-version-cutover?bucket=mybucket")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "mybucket", gotBucket)
}

// TestVerifyPerVersionCutoverHandler_Error proves the handler returns 500 on
// a backend error.
func TestVerifyPerVersionCutoverHandler_Error(t *testing.T) {
	sock := udsTestSocket(t)
	s := &Server{
		verifyPerVersionCutoverFn: func(_ context.Context, _ string) (PerVersionCutoverReadiness, error) {
			return PerVersionCutoverReadiness{}, errors.New("backend exploded")
		},
	}
	cli := startUDSAdminTestServerWithSrv(t, sock, s)

	resp, err := cli.Get("http://unix/v1/cluster/verify-per-version-cutover")
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode, "body: %s", body)
}
