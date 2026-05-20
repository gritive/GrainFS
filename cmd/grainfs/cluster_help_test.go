package main

import (
	"bytes"
	"os"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// TestClusterHelpGolden is the C1 contract guard for the `grainfs cluster`
// command tree and the top-level `grainfs join` shortcut. Any change to a
// subcommand name, flag name, default, hidden status, deprecation, or help
// text fails this test. Treat failures as a CLI contract change — most
// updates here need a CHANGELOG entry. Regenerate via the capture block
// in the "Regenerate goldens:" section below.
//
// Regenerate goldens:
//
//	./bin/grainfs cluster --help                       > cmd/grainfs/testdata/cluster_help.golden 2>&1
//	./bin/grainfs cluster config --help                > cmd/grainfs/testdata/cluster_config_help.golden 2>&1
//	./bin/grainfs cluster config set --help            > cmd/grainfs/testdata/cluster_config_set_help.golden 2>&1
//	./bin/grainfs cluster config reset --help          > cmd/grainfs/testdata/cluster_config_reset_help.golden 2>&1
//	./bin/grainfs cluster rotate-key --help            > cmd/grainfs/testdata/cluster_rotate_key_help.golden 2>&1
//	./bin/grainfs cluster rotate-key begin --help      > cmd/grainfs/testdata/cluster_rotate_key_begin_help.golden 2>&1
//	./bin/grainfs cluster balancer --help              > cmd/grainfs/testdata/cluster_balancer_help.golden 2>&1
//	./bin/grainfs cluster join --help                  > cmd/grainfs/testdata/cluster_join_help.golden 2>&1
//	./bin/grainfs cluster remove-peer --help           > cmd/grainfs/testdata/cluster_remove_peer_help.golden 2>&1
//	./bin/grainfs cluster drain --help                 > cmd/grainfs/testdata/cluster_drain_help.golden 2>&1
//	./bin/grainfs join --help                          > cmd/grainfs/testdata/join_help.golden 2>&1
func TestClusterHelpGolden(t *testing.T) {
	cases := []struct {
		name   string
		args   []string
		golden string
	}{
		{"cluster", []string{"cluster", "--help"}, "testdata/cluster_help.golden"},
		{"cluster_config", []string{"cluster", "config", "--help"}, "testdata/cluster_config_help.golden"},
		{"cluster_config_set", []string{"cluster", "config", "set", "--help"}, "testdata/cluster_config_set_help.golden"},
		{"cluster_config_reset", []string{"cluster", "config", "reset", "--help"}, "testdata/cluster_config_reset_help.golden"},
		{"cluster_rotate_key", []string{"cluster", "rotate-key", "--help"}, "testdata/cluster_rotate_key_help.golden"},
		{"cluster_rotate_key_begin", []string{"cluster", "rotate-key", "begin", "--help"}, "testdata/cluster_rotate_key_begin_help.golden"},
		{"cluster_balancer", []string{"cluster", "balancer", "--help"}, "testdata/cluster_balancer_help.golden"},
		{"cluster_join", []string{"cluster", "join", "--help"}, "testdata/cluster_join_help.golden"},
		{"cluster_remove_peer", []string{"cluster", "remove-peer", "--help"}, "testdata/cluster_remove_peer_help.golden"},
		{"cluster_drain", []string{"cluster", "drain", "--help"}, "testdata/cluster_drain_help.golden"},
		{"join", []string{"join", "--help"}, "testdata/join_help.golden"},
	}

	// Walk the cluster + join command trees so cleanup hits every singleton
	// cobra may have parsed --help on, including subcommands built by
	// factory functions (clusterJoinCmd(), clusterRotateKeyCmd(), etc.)
	// that we cannot name directly.
	var allCmds []*cobra.Command
	collect := func(root *cobra.Command) {
		var walk func(*cobra.Command)
		walk = func(c *cobra.Command) {
			allCmds = append(allCmds, c)
			for _, sub := range c.Commands() {
				walk(sub)
			}
		}
		walk(root)
	}
	collect(rootCmd)

	resetHelpFlag := func(c *cobra.Command) {
		if f := c.Flags().Lookup("help"); f != nil {
			_ = f.Value.Set("false")
			f.Changed = false
		}
	}

	// Cobra persists --help on the command it parsed; if left set, sibling
	// tests that re-run Execute() short-circuit into help output instead
	// of running RunE. Reset every command we walked.
	t.Cleanup(func() {
		rootCmd.SetArgs(nil)
		rootCmd.SetOut(nil)
		rootCmd.SetErr(nil)
		for _, c := range allCmds {
			c.SetOut(nil)
			c.SetErr(nil)
			resetHelpFlag(c)
		}
	})

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			// Pin output on rootCmd plus every walked command. Sibling
			// tests (e.g. cluster_config_test.go) call SetOut on
			// subcommand singletons directly, which would otherwise
			// capture our help output silently.
			rootCmd.SetOut(&buf)
			rootCmd.SetErr(&buf)
			for _, c := range allCmds {
				c.SetOut(&buf)
				c.SetErr(&buf)
			}
			rootCmd.SetArgs(tc.args)
			_ = rootCmd.Execute()

			golden, err := os.ReadFile(tc.golden)
			require.NoError(t, err)
			require.Equal(t, string(golden), buf.String(),
				"%s --help output changed; if intentional, regenerate %s and update CHANGELOG",
				tc.name, tc.golden)
		})
	}
}
