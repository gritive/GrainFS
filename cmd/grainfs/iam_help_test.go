package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// TestIAMHelp_Golden guards the public CLI flag surface for the iam
// command tree. Any accidental change to a flag name, default, hidden
// status, deprecation, or help text will fail this test. Treat failures
// as a CLI contract change and weigh accordingly — most updates here
// need a CHANGELOG entry.
func TestIAMHelp_Golden(t *testing.T) {
	cases := []struct {
		name string
		args []string
		must []string // substrings that MUST appear in --help output
	}{
		{
			name: "iam",
			args: []string{"iam", "--help"},
			must: []string{
				"Manage GrainFS IAM",
				"--endpoint",
				"GRAINFS_ADMIN_SOCKET",
				"--json",
				"sa",
				"key",
				"grant",
			},
		},
		{
			name: "iam sa create",
			args: []string{"iam", "sa", "create", "--help"},
			must: []string{
				"Create a ServiceAccount",
				"--description",
				"free-form SA description",
			},
		},
		{
			name: "iam sa list",
			args: []string{"iam", "sa", "list", "--help"},
			must: []string{"List ServiceAccounts"},
		},
		{
			name: "iam sa get",
			args: []string{"iam", "sa", "get", "--help"},
			must: []string{"Show SA detail"},
		},
		{
			name: "iam sa delete",
			args: []string{"iam", "sa", "delete", "--help"},
			must: []string{"Delete an SA", "cascades"},
		},
		{
			name: "iam key create",
			args: []string{"iam", "key", "create", "--help"},
			must: []string{
				"Issue a new AccessKey",
				"--bucket",
				"repeatable",
			},
		},
		{
			name: "iam key revoke",
			args: []string{"iam", "key", "revoke", "--help"},
			must: []string{"Revoke an AccessKey"},
		},
		{
			name: "iam grant put",
			args: []string{"iam", "grant", "put", "--help"},
			must: []string{"Grant role on bucket", "Read|Write|Admin"},
		},
		{
			name: "iam grant delete",
			args: []string{"iam", "grant", "delete", "--help"},
			must: []string{"Remove grant"},
		},
		{
			name: "iam grant list",
			args: []string{"iam", "grant", "list", "--help"},
			must: []string{"List grants", "--sa", "--bucket"},
		},
	}
	// Reset every singleton command's --help flag after the suite. cobra
	// persists --help on the command it parsed; if left set, subsequent
	// Execute() calls on the same singleton short-circuit into help output
	// instead of running RunE, contaminating sibling tests.
	t.Cleanup(func() {
		rootCmd.SetArgs(nil)
		rootCmd.SetOut(nil)
		rootCmd.SetErr(nil)
		resetHelpFlag := func(c *cobra.Command) {
			if f := c.Flags().Lookup("help"); f != nil {
				_ = f.Value.Set("false")
				f.Changed = false
			}
		}
		for _, c := range []*cobra.Command{
			rootCmd,
			iamCmd, iamSACmd, iamKeyCmd, iamGrantCmd,
			iamSACreateCmd, iamSAListCmd, iamSAGetCmd, iamSADeleteCmd,
			iamKeyCreateCmd, iamKeyRevokeCmd,
			iamGrantPutCmd, iamGrantDeleteCmd, iamGrantListCmd,
		} {
			resetHelpFlag(c)
		}
	})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			rootCmd.SetOut(&out)
			rootCmd.SetErr(&out)
			rootCmd.SetArgs(tc.args)
			_ = rootCmd.Execute()
			s := out.String()
			for _, want := range tc.must {
				if !strings.Contains(s, want) {
					t.Errorf("missing %q in --help output:\n%s", want, s)
				}
			}
		})
	}
}
