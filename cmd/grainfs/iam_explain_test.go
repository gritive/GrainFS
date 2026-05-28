package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func resetExplainFlags() {
	for _, name := range []string{"sa", "s3"} {
		f := iamExplainCmd.Flags().Lookup(name)
		if f != nil {
			_ = f.Value.Set(f.DefValue)
			f.Changed = false
		}
	}
}

func TestCLI_IAMExplainS3(t *testing.T) {
	resetExplainFlags()
	t.Cleanup(resetExplainFlags)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/policy/simulate", func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		var req struct {
			SAID     string `json:"sa_id"`
			Action   string `json:"action"`
			Resource string `json:"resource"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		require.Equal(t, "sa-x", req.SAID)
		require.Equal(t, "s3:PutObject", req.Action)
		require.Equal(t, "arn:aws:s3:::logs/2026/entry.json", req.Resource)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"effect":"Allow","matched_policy":"writer","reason":"explicit Allow"}`)
	})
	sock := startFakeAdminUDS(t, mux)

	var out bytes.Buffer
	rootCmd.SetArgs([]string{
		"iam", "--endpoint", sock,
		"explain",
		"--sa", "sa-x",
		"--s3", "put", "s3://logs/2026/entry.json",
	})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	err := rootCmd.Execute()
	require.NoError(t, err, "output: %s", out.String())
	require.Contains(t, out.String(), "Action:         s3:PutObject")
	require.Contains(t, out.String(), "Effect:         Allow")
}
