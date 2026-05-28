package iamadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExplainS3Request_ObjectVerb(t *testing.T) {
	req, err := explainS3Request("put", "s3://logs/2026/entry.json")
	require.NoError(t, err)
	require.Equal(t, "s3:PutObject", req.Action)
	require.Equal(t, "arn:aws:s3:::logs/2026/entry.json", req.Resource)
}

func TestExplainS3Request_ListVerb(t *testing.T) {
	req, err := explainS3Request("ls", "s3://logs")
	require.NoError(t, err)
	require.Equal(t, "s3:ListBucket", req.Action)
	require.Equal(t, "arn:aws:s3:::logs", req.Resource)
}

func TestExplainS3Request_Invalid(t *testing.T) {
	_, err := explainS3Request("copy", "s3://logs/key")
	require.ErrorContains(t, err, "unsupported S3 verb")

	_, err = explainS3Request("put", "logs/key")
	require.ErrorContains(t, err, "must start with s3://")
}

func TestRunExplain_Text(t *testing.T) {
	var got PolicySimulateRequest
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/policy/simulate", func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"effect":"Allow","matched_policy":"writer","matched_sid":"sid1","reason":"explicit Allow"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var out bytes.Buffer
	err := RunExplain(context.Background(), ExplainOptions{
		BaseOptions: BaseOptions{Endpoint: srv.URL, Stdout: &out},
		SAID:        "sa-x",
		S3Verb:      "put",
		S3URI:       "s3://logs/2026/entry.json",
	})
	require.NoError(t, err)
	require.Equal(t, PolicySimulateRequest{
		SAID:     "sa-x",
		Action:   "s3:PutObject",
		Resource: "arn:aws:s3:::logs/2026/entry.json",
	}, got)
	require.Contains(t, out.String(), "Action:         s3:PutObject")
	require.Contains(t, out.String(), "Resource:       arn:aws:s3:::logs/2026/entry.json")
	require.Contains(t, out.String(), "Effect:         Allow")
}

func TestRunExplain_JSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/policy/simulate", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"effect":"Deny","reason":"implicit Deny"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var out bytes.Buffer
	err := RunExplain(context.Background(), ExplainOptions{
		BaseOptions: BaseOptions{Endpoint: srv.URL, JSONOut: true, Stdout: &out},
		SAID:        "sa-x",
		S3Verb:      "ls",
		S3URI:       "s3://logs",
	})
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(out.String(), "\n"))

	var got ExplainResponse
	require.NoError(t, json.Unmarshal(out.Bytes(), &got))
	require.Equal(t, "s3:ListBucket", got.Request.Action)
	require.Equal(t, "arn:aws:s3:::logs", got.Request.Resource)
	require.Equal(t, "Deny", got.Simulation.Effect)
}
