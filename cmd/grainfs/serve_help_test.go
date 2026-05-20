package main

import (
	"bytes"
	"os"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// TestServeHelpGolden is the C1 contract guard for `grainfs serve --help`.
// Any change to a flag name, default, hidden status, deprecation, or help
// text fails this test. Treat failures as a CLI contract change — most
// updates here need a CHANGELOG entry. Regenerate via:
//
//	./bin/grainfs serve --help > cmd/grainfs/testdata/serve_help.golden 2>&1
func TestServeHelpGolden(t *testing.T) {
	t.Cleanup(func() {
		rootCmd.SetArgs(nil)
		rootCmd.SetOut(nil)
		rootCmd.SetErr(nil)
		serveCmd.SetOut(nil)
		serveCmd.SetErr(nil)
		resetHelpFlag := func(c *cobra.Command) {
			if f := c.Flags().Lookup("help"); f != nil {
				_ = f.Value.Set("false")
				f.Changed = false
			}
		}
		for _, c := range []*cobra.Command{rootCmd, serveCmd} {
			resetHelpFlag(c)
		}
	})

	// Pin serveCmd's output streams as well as rootCmd's: sibling tests
	// (e.g. TestServeHelpDoesNotExposeNoEncryption) call serveCmd.SetOut
	// directly with their own buffer, which would otherwise capture our
	// help output silently.
	var buf bytes.Buffer
	rootCmd.SetOut(&buf)
	rootCmd.SetErr(&buf)
	serveCmd.SetOut(&buf)
	serveCmd.SetErr(&buf)
	rootCmd.SetArgs([]string{"serve", "--help"})
	_ = rootCmd.Execute()

	golden, err := os.ReadFile("testdata/serve_help.golden")
	require.NoError(t, err)
	require.Equal(t, string(golden), buf.String(),
		"serve --help output changed; if intentional, regenerate "+
			"cmd/grainfs/testdata/serve_help.golden and update CHANGELOG")
}
