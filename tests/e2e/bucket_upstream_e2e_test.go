package e2e

import (
	"context"
	"fmt"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBucketUpstreamE2E exercises the `grainfs bucket upstream …` admin CLI
// surface (put/get/list/delete roundtrip + legacy-path removal) against both
// fixtures. Uses the shared single + shared cluster fixtures — bucket
// upstream records are namespaced per name, so sub-tests pick a unique name.
func TestBucketUpstreamE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runBucketUpstreamCases(t, newSingleNodeS3Target())
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runBucketUpstreamCases(t, newSharedClusterS3Target(t))
	})
}

func runBucketUpstreamCases(t *testing.T, tgt s3Target) {
	t.Helper()
	binary := getBinary()
	sock := tgt.adminSockPath()

	// LegacyCLI_Removed first — fixture-independent, no state mutation.
	t.Run("LegacyCLI_Removed", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		out, _ := exec.CommandContext(ctx, binary, "iam", "--help").CombinedOutput()
		require.NotContains(t, string(out), "bucket-upstream",
			"expected 'bucket-upstream' to be absent from `grainfs iam --help`")

		ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel2()
		legacy := exec.CommandContext(ctx2, binary, "iam", "bucket-upstream", "set", "xb",
			"--endpoint", "/tmp/nonexistent.sock",
			"--endpoint-url", "http://x:1",
			"--access-key", "x",
			"--secret-key", "x",
		)
		out, err := legacy.CombinedOutput()
		require.Error(t, err, "legacy `iam bucket-upstream set …` must exit non-zero; got output: %s", out)
	})

	t.Run("CLIRoundtrip", func(t *testing.T) {
		name := fmt.Sprintf("up-%s-%d", tgt.name, time.Now().UnixNano())

		// PUT
		{
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			putCmd := exec.CommandContext(ctx, binary, "bucket", "upstream", "put", name,
				"--endpoint", sock,
				"--endpoint-url", "http://upstream.example:9000",
				"--access-key", "AKUP",
				"--secret-key", "upstream-secret-plain",
			)
			out, err := putCmd.CombinedOutput()
			require.NoError(t, err, "put: %s", string(out))
		}

		// GET — record present, secret_key absent
		{
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			out, err := exec.CommandContext(ctx, binary, "bucket", "--json", "upstream", "get", name,
				"--endpoint", sock,
			).CombinedOutput()
			require.NoError(t, err, "get: %s", string(out))
			body := string(out)
			assert.Contains(t, body, `"upstream_url":"http://upstream.example:9000"`, "wire JSON must use upstream_url")
			assert.Contains(t, body, `"access_key":"AKUP"`)
			assert.NotContains(t, body, "upstream-secret-plain", "GET response must not leak plaintext secret")
			assert.NotContains(t, body, `"secret_key"`, "GET response must not include secret_key field")
		}

		// LIST — name in array
		{
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			out, err := exec.CommandContext(ctx, binary, "bucket", "--json", "upstream", "list",
				"--endpoint", sock,
			).CombinedOutput()
			require.NoError(t, err, "list: %s", string(out))
			body := string(out)
			assert.Contains(t, body, fmt.Sprintf(`"bucket":%q`, name), "list must include the registered bucket")
			assert.NotContains(t, body, "upstream-secret-plain", "list must not leak plaintext")
		}

		// DELETE
		{
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			out, err := exec.CommandContext(ctx, binary, "bucket", "upstream", "delete", name,
				"--endpoint", sock,
			).CombinedOutput()
			require.NoError(t, err, "delete: %s", string(out))
		}

		// GET after delete → 404
		{
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			out, err := exec.CommandContext(ctx, binary, "bucket", "upstream", "get", name,
				"--endpoint", sock,
			).CombinedOutput()
			require.Error(t, err, "GET after delete must fail; output: %s", string(out))
			assert.Contains(t, string(out), "not found", "post-delete GET must surface not found in output")
		}
	})
}
