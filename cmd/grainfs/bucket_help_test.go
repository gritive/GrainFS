package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// TestBucketHelp_Golden guards the public CLI flag surface for the bucket
// command tree. Any accidental change to a flag name, default, hidden
// status, deprecation, or help text fails this test. Treat failures as
// a CLI contract change and weigh accordingly — most updates here need
// a CHANGELOG entry.
func TestBucketHelp_Golden(t *testing.T) {
	cases := []struct {
		name string
		args []string
		must []string // substrings that MUST appear in --help output
	}{
		{
			name: "bucket",
			args: []string{"bucket", "--help"},
			must: []string{
				"Bucket-scoped admin commands",
				"--endpoint",
				"GRAINFS_ADMIN_SOCKET",
				"--json",
				"create", "list", "info", "delete",
				"upstream", "policy", "versioning",
			},
		},
		{
			name: "bucket create",
			args: []string{"bucket", "create", "--help"},
			must: []string{
				"Create a bucket",
				"--attach-sa",
				"--attach-policy",
				"grainfs bucket create my-bucket",
			},
		},
		{
			name: "bucket list",
			args: []string{"bucket", "list", "--help"},
			must: []string{
				"List buckets",
				"__grainfs_*",
				"grainfs bucket list",
			},
		},
		{
			name: "bucket info",
			args: []string{"bucket", "info", "--help"},
			must: []string{
				"Show bucket details",
				"object count",
				"info <name>",
			},
		},
		{
			name: "bucket delete",
			args: []string{"bucket", "delete", "--help"},
			must: []string{
				"Delete a bucket",
				"--force",
				"--recursive",
				"must be empty unless --force",
			},
		},
		{
			name: "bucket upstream",
			args: []string{"bucket", "upstream", "--help"},
			must: []string{
				"Manage per-bucket pull-through upstream credentials",
				"put", "get", "list", "delete",
			},
		},
		{
			name: "bucket upstream put",
			args: []string{"bucket", "upstream", "put", "--help"},
			must: []string{
				"Register or rotate the upstream credentials",
				"--scheme",
				"--endpoint-url",
				"--access-key",
				"--secret-key",
				"--region",
				"--remote-bucket",
			},
		},
		{
			name: "bucket upstream get",
			args: []string{"bucket", "upstream", "get", "--help"},
			must: []string{
				"Show the upstream config for a bucket",
				"secret_key never returned",
				"get <bucket>",
			},
		},
		{
			name: "bucket upstream list",
			args: []string{"bucket", "upstream", "list", "--help"},
			must: []string{
				"List all bucket-upstream configurations",
				"grainfs bucket upstream list",
			},
		},
		{
			name: "bucket upstream delete",
			args: []string{"bucket", "upstream", "delete", "--help"},
			must: []string{
				"Remove the upstream config for a bucket",
				"delete <bucket>",
			},
		},
		{
			name: "bucket policy",
			args: []string{"bucket", "policy", "--help"},
			must: []string{
				"Manage bucket policies",
				"get", "set", "delete",
			},
		},
		{
			name: "bucket policy get",
			args: []string{"bucket", "policy", "get", "--help"},
			must: []string{
				"Get the bucket policy",
				"raw JSON document",
				"get <bucket>",
			},
		},
		{
			name: "bucket policy set",
			args: []string{"bucket", "policy", "set", "--help"},
			must: []string{
				"Set the bucket policy",
				"JSON file or stdin",
				"set <bucket> <policy-file|->",
				"cat policy.json | grainfs bucket policy set my-bucket -",
			},
		},
		{
			name: "bucket policy delete",
			args: []string{"bucket", "policy", "delete", "--help"},
			must: []string{
				"Remove the bucket policy",
				"delete <bucket>",
			},
		},
		{
			name: "bucket versioning",
			args: []string{"bucket", "versioning", "--help"},
			must: []string{
				"Manage bucket versioning state",
				"get", "enable", "suspend",
			},
		},
		{
			name: "bucket versioning get",
			args: []string{"bucket", "versioning", "get", "--help"},
			must: []string{
				"Get the current versioning state of a bucket",
				"get <bucket>",
			},
		},
		{
			name: "bucket versioning enable",
			args: []string{"bucket", "versioning", "enable", "--help"},
			must: []string{
				"Enable versioning for a bucket",
				"enable <bucket>",
			},
		},
		{
			name: "bucket versioning suspend",
			args: []string{"bucket", "versioning", "suspend", "--help"},
			must: []string{
				"Suspend versioning for a bucket",
				"suspend <bucket>",
			},
		},
	}

	// MAINTENANCE: keep this list in sync with the bucket command tree
	// in cmd/grainfs/bucket*.go. When a new bucket subcommand lands,
	// add its var here so the help-flag state is reset between
	// subtests. Cobra's --help flag is sticky per command singleton —
	// an unreset flag contaminates sibling tests.
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
			bucketCmd,
			bucketCreateCmd, bucketListCmd, bucketInfoCmd, bucketDeleteCmd,
			bucketUpstreamCmd,
			bucketUpstreamPutCmd, bucketUpstreamGetCmd, bucketUpstreamListCmd, bucketUpstreamDeleteCmd,
			bucketPolicyCmd,
			bucketPolicyGetCmd, bucketPolicySetCmd, bucketPolicyDeleteCmd,
			bucketVersioningCmd,
			bucketVersioningGetCmd, bucketVersioningEnableCmd, bucketVersioningSuspendCmd,
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
