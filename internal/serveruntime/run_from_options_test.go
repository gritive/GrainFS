package serveruntime

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRunFromOptionsRejectsMissingEncryptionKeyFile verifies that an explicit
// missing --encryption-key-file path is treated as an error (would otherwise
// silently auto-generate and orphan existing shards). The error is wrapped
// with the "encryption setup" prefix that runServe used historically.
func TestRunFromOptionsRejectsMissingEncryptionKeyFile(t *testing.T) {
	opts := ServeOptions{
		DataDir:           t.TempDir(),
		EncryptionKeyFile: filepath.Join(t.TempDir(), "missing.key"),
		Port:              0,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel so even if Run starts it exits fast
	err := RunFromOptions(ctx, opts)
	require.Error(t, err)
	require.Contains(t, err.Error(), "encryption setup")
}
