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

// TestBucketUpstream_CLIRoundtrip verifies that `grainfs bucket upstream
// put/get/list/delete` actually talk to the admin UDS end-to-end. This guards
// against regressions in the CLI -> JSON shape -> handler chain (per
// /plan-eng-review override A7g).
func TestBucketUpstream_CLIRoundtrip(t *testing.T) {
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

	// PUT — registers a bucket upstream record. Secret is fed via stdin.
	{
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		putCmd := exec.CommandContext(ctx, binary, "bucket", "upstream", "put", "shared",
			"--endpoint", sock,
			"--upstream-url", "http://upstream.example:9000",
			"--access-key", "AKUP",
			"--secret-key-stdin",
		)
		putCmd.Stdin = strings.NewReader("upstream-secret-plain\n")
		out, err := putCmd.CombinedOutput()
		require.NoError(t, err, "put: %s", string(out))
	}

	// GET — confirm record present, secret_key NOT in response.
	{
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, binary, "bucket", "upstream", "get", "shared",
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
		out, err := exec.CommandContext(ctx, binary, "bucket", "upstream", "list",
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
		out, err := exec.CommandContext(ctx, binary, "bucket", "upstream", "delete", "shared",
			"--endpoint", sock,
		).CombinedOutput()
		require.NoError(t, err, "delete: %s", string(out))
	}

	{
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, binary, "bucket", "upstream", "get", "shared",
			"--endpoint", sock,
		).CombinedOutput()
		require.Error(t, err, "GET after delete must fail; output: %s", string(out))
		assert.Contains(t, string(out), "404", "post-delete GET must surface 404 in output")
	}
}

// TestBucketUpstream_LegacyCLI_Removed asserts that `grainfs iam bucket-upstream …`
// no longer exists. Regression test for ADR 0010 surface relocation: the old
// CLI path was a real interface in v0.0.123.0–v0.0.131.0; users with scripts
// depend on the failure mode being clear.
//
// Cobra v1.10+ shows parent help (exit 0) for unknown subcommands on group
// commands. The meaningful assertion is that "bucket-upstream" is NOT listed
// as an available subcommand under `grainfs iam`.
func TestBucketUpstream_LegacyCLI_Removed(t *testing.T) {
	binary := getBinary()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// `grainfs iam --help` must NOT mention "bucket-upstream".
	out, _ := exec.CommandContext(ctx, binary, "iam", "--help").CombinedOutput()
	if strings.Contains(string(out), "bucket-upstream") {
		t.Fatalf("expected 'bucket-upstream' to be absent from `grainfs iam --help`, got: %s", out)
	}
}
