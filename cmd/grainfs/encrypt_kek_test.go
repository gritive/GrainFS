package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// resetEncryptKEKFlags resets all singleton flags modified during a test so
// subsequent tests in the same process are not contaminated.
func resetEncryptKEKFlags(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		rootCmd.SetArgs(nil)
		rootCmd.SetOut(nil)
		rootCmd.SetErr(nil)
		rotateIKnow = false
		retireVersion = 0
		retireConfirm = ""
		retireClusterName = ""
		pruneVersion = 0
		pruneConfirm = ""
		pruneClusterName = ""
		for _, c := range []*cobra.Command{
			rootCmd,
			encryptCmd,
			encryptKEKCmd,
			encryptKEKRotateCmd,
			encryptKEKRetireCmd,
			encryptKEKPruneCmd,
			encryptKEKStatusCmd,
		} {
			if f := c.Flags().Lookup("help"); f != nil {
				_ = f.Value.Set("false")
				f.Changed = false
			}
		}
	})
}

// runEncryptKEKCmd executes rootCmd with the given args and returns combined
// stdout+stderr output. The error is from cmd.Execute(), which is nil for --help.
func runEncryptKEKCmd(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var buf bytes.Buffer
	rootCmd.SetOut(&buf)
	rootCmd.SetErr(&buf)
	rootCmd.SetArgs(args)
	err := rootCmd.Execute()
	return buf.String(), err
}

// TestCLI_Encrypt_KEK_Help verifies that `encrypt kek --help` lists all four
// subcommands.
func TestCLI_Encrypt_KEK_Help(t *testing.T) {
	resetEncryptKEKFlags(t)
	out, _ := runEncryptKEKCmd(t, "encrypt", "kek", "--help")
	for _, want := range []string{"rotate", "retire", "prune", "status"} {
		if !strings.Contains(out, want) {
			t.Errorf("help output missing %q:\n%s", want, out)
		}
	}
}

// TestCLI_Encrypt_KEK_Rotate_RequiresIKnow verifies that omitting --i-know
// fails with a hint referencing the flag name.
func TestCLI_Encrypt_KEK_Rotate_RequiresIKnow(t *testing.T) {
	resetEncryptKEKFlags(t)
	// No --endpoint needed: the --i-know check happens before any network call.
	out, err := runEncryptKEKCmd(t, "encrypt", "kek", "rotate")
	if err == nil {
		t.Fatalf("expected error without --i-know; got no error. output:\n%s", out)
	}
	if !strings.Contains(out+err.Error(), "--i-know") {
		t.Errorf("expected --i-know hint; got:\nout=%s\nerr=%v", out, err)
	}
}

// TestCLI_Encrypt_KEK_Prune_RequiresVersionAndConfirm verifies that passing
// --version without --confirm-name fails with a descriptive error.
func TestCLI_Encrypt_KEK_Prune_RequiresVersionAndConfirm(t *testing.T) {
	resetEncryptKEKFlags(t)
	// version supplied, confirm-name missing → MarkFlagRequired fires before RunE.
	out, err := runEncryptKEKCmd(t, "encrypt", "kek", "prune",
		"--version", "3",
		"--cluster-name", "my-node",
	)
	if err == nil {
		t.Fatalf("expected error without --confirm-name; output:\n%s", out)
	}
	combined := out + err.Error()
	if !strings.Contains(combined, "confirm-name") {
		t.Errorf("expected confirm-name in error; got:\nout=%s\nerr=%v", out, err)
	}
}

// TestCLI_Encrypt_KEK_Retire_RequiresClusterName verifies that omitting
// --cluster-name from retire fails before any network call.
func TestCLI_Encrypt_KEK_Retire_RequiresClusterName(t *testing.T) {
	resetEncryptKEKFlags(t)
	out, err := runEncryptKEKCmd(t, "encrypt", "kek", "retire",
		"--version", "2",
		"--confirm-name", "delete-permanently-2",
	)
	if err == nil {
		t.Fatalf("expected error without --cluster-name; output:\n%s", out)
	}
	combined := out + err.Error()
	if !strings.Contains(combined, "cluster-name") {
		t.Errorf("expected cluster-name in error; got:\nout=%s\nerr=%v", out, err)
	}
}

// TestCLI_Encrypt_KEK_Retire_ClusterNameMismatch verifies that passing a
// --cluster-name that does not match the actual node_id from /v1/cluster/status
// is rejected BEFORE sending the retire request to /v1/encrypt/kek/retire.
func TestCLI_Encrypt_KEK_Retire_ClusterNameMismatch(t *testing.T) {
	resetEncryptKEKFlags(t)

	var retireCalled bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster/status", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"mode":    "cluster",
			"node_id": "real-cluster",
		})
	})
	mux.HandleFunc("/v1/encrypt/kek/retire", func(w http.ResponseWriter, r *http.Request) {
		retireCalled = true
		w.WriteHeader(http.StatusOK)
	})
	sock := startFakeAdminUDS(t, mux)

	out, err := runEncryptKEKCmd(t, "encrypt", "kek", "retire",
		"--endpoint", sock,
		"--version", "2",
		"--confirm-name", "delete-permanently-2",
		"--cluster-name", "wrong-cluster",
	)
	if err == nil {
		t.Fatalf("expected error on cluster-name mismatch; output:\n%s", out)
	}
	if retireCalled {
		t.Error("retire endpoint must NOT be called when cluster-name mismatches")
	}
	combined := out + err.Error()
	if !strings.Contains(combined, "real-cluster") {
		t.Errorf("error should mention actual name %q; got:\nout=%s\nerr=%v", "real-cluster", out, err)
	}
	if !strings.Contains(combined, "wrong-cluster") {
		t.Errorf("error should mention supplied name %q; got:\nout=%s\nerr=%v", "wrong-cluster", out, err)
	}
}
