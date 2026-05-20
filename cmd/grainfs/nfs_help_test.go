package main

import (
	"bytes"
	"os"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// TestNfsHelpGolden is the C1 contract guard for the `grainfs nfs` command
// tree. Any change to a subcommand name, flag name, default, hidden status,
// deprecation, or help text fails this test. Treat failures as a CLI
// contract change — most updates here need a CHANGELOG entry. Regenerate
// via the capture block in the "Regenerate goldens:" section below.
//
// Regenerate goldens:
//
//	./bin/grainfs nfs --help                 > cmd/grainfs/testdata/nfs_help.golden 2>&1
//	./bin/grainfs nfs export --help          > cmd/grainfs/testdata/nfs_export_help.golden 2>&1
//	./bin/grainfs nfs export add --help      > cmd/grainfs/testdata/nfs_export_add_help.golden 2>&1
//	./bin/grainfs nfs export remove --help   > cmd/grainfs/testdata/nfs_export_remove_help.golden 2>&1
//	./bin/grainfs nfs export update --help   > cmd/grainfs/testdata/nfs_export_update_help.golden 2>&1
//	./bin/grainfs nfs export list --help     > cmd/grainfs/testdata/nfs_export_list_help.golden 2>&1
//	./bin/grainfs nfs debug --help           > cmd/grainfs/testdata/nfs_debug_help.golden 2>&1
func TestNfsHelpGolden(t *testing.T) {
	cases := []struct {
		name   string
		args   []string
		golden string
	}{
		{"nfs", []string{"nfs", "--help"}, "testdata/nfs_help.golden"},
		{"nfs_export", []string{"nfs", "export", "--help"}, "testdata/nfs_export_help.golden"},
		{"nfs_export_add", []string{"nfs", "export", "add", "--help"}, "testdata/nfs_export_add_help.golden"},
		{"nfs_export_remove", []string{"nfs", "export", "remove", "--help"}, "testdata/nfs_export_remove_help.golden"},
		{"nfs_export_update", []string{"nfs", "export", "update", "--help"}, "testdata/nfs_export_update_help.golden"},
		{"nfs_export_list", []string{"nfs", "export", "list", "--help"}, "testdata/nfs_export_list_help.golden"},
		{"nfs_debug", []string{"nfs", "debug", "--help"}, "testdata/nfs_debug_help.golden"},
	}

	// Walk the nfs command tree so cleanup hits every singleton cobra may
	// have parsed --help on, including subcommands built by factory
	// functions (nfsExportCmd(), nfsDebugCmd(), etc.) that we cannot name
	// directly.
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
			// tests call SetOut on subcommand singletons directly, which
			// would otherwise capture our help output silently.
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
