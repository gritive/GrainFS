package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIAMBucketUpstream_CLIRoundtrip verifies that `grainfs iam bucket-upstream
// set/get/list/delete` actually talk to the admin UDS end-to-end. This guards
// against regressions in the CLI -> JSON shape -> handler chain (per
// /plan-eng-review override A7g).
func TestIAMBucketUpstream_CLIRoundtrip(t *testing.T) {
	binary := getBinary()

	dir, err := os.MkdirTemp("", "grainfs-iam-bu-cli-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	port := freePort()
	cmd := exec.Command(binary, "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
		"--snapshot-interval", "0",
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	defer terminateProcess(cmd)

	waitForPort(t, port, 30*time.Second)
	_, _ = bootstrapAdminViaUDS(t, dir) // discard creds — CLI tests don't need S3 sigv4 here

	sock := filepath.Join(dir, "admin.sock")

	// SET — registers a bucket-upstream record. Secret is fed via stdin.
	{
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		setCmd := exec.CommandContext(ctx, binary, "iam", "bucket-upstream", "set", "shared",
			"--endpoint", sock,
			"--upstream-url", "http://upstream.example:9000",
			"--access-key", "AKUP",
			"--secret-key-stdin",
		)
		setCmd.Stdin = strings.NewReader("upstream-secret-plain\n")
		out, err := setCmd.CombinedOutput()
		require.NoError(t, err, "set: %s", string(out))
	}

	// GET — confirm record present, secret_key NOT in response.
	{
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, binary, "iam", "bucket-upstream", "get", "shared",
			"--endpoint", sock,
		).CombinedOutput()
		require.NoError(t, err, "get: %s", string(out))
		body := string(out)
		assert.Contains(t, body, `"upstream_url":"http://upstream.example:9000"`, "wire JSON must use upstream_url")
		assert.Contains(t, body, `"access_key":"AKUP"`)
		assert.NotContains(t, body, "upstream-secret-plain", "GET response must not leak plaintext secret")
		assert.NotContains(t, body, `"secret_key"`, "GET response must not include secret_key field")
	}

	// LIST — confirm "shared" is in the array.
	{
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, binary, "iam", "bucket-upstream", "list",
			"--endpoint", sock,
		).CombinedOutput()
		require.NoError(t, err, "list: %s", string(out))
		body := string(out)
		assert.Contains(t, body, `"bucket":"shared"`, "list must include the registered bucket")
		assert.NotContains(t, body, "upstream-secret-plain", "list must not leak plaintext")
	}

	// DELETE — record gone, GET returns 404 (CLI exits non-zero).
	{
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, binary, "iam", "bucket-upstream", "delete", "shared",
			"--endpoint", sock,
		).CombinedOutput()
		require.NoError(t, err, "delete: %s", string(out))
	}

	{
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, binary, "iam", "bucket-upstream", "get", "shared",
			"--endpoint", sock,
		).CombinedOutput()
		require.Error(t, err, "GET after delete must fail; output: %s", string(out))
		assert.Contains(t, string(out), "404", "post-delete GET must surface 404 in output")
	}
}
