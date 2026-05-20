package main

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/bucketadmin"
)

var bucketPolicyParentCmd = &cobra.Command{
	Use:   "policy",
	Short: "Manage bucket policies",
}

var bucketPolicyGetCmd = &cobra.Command{
	Use:   "get <bucket>",
	Short: "Get the bucket policy (raw JSON document)",
	Args:  cobra.ExactArgs(1),
	Example: `  grainfs bucket policy get my-bucket
  grainfs bucket --json policy get my-bucket`,
	RunE: func(c *cobra.Command, args []string) error {
		base, err := bucketBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return bucketadmin.RunPolicyGet(c.Context(), bucketadmin.PolicyGetOptions{
			BaseOptions: base, Bucket: args[0],
		})
	},
}

var bucketPolicySetCmd = &cobra.Command{
	Use:   "set <bucket> <policy-file|->",
	Short: `Set the bucket policy from a JSON file or stdin ("-")`,
	Args:  cobra.ExactArgs(2),
	Example: `  grainfs bucket policy set my-bucket policy.json
  cat policy.json | grainfs bucket policy set my-bucket -`,
	RunE: func(c *cobra.Command, args []string) error {
		base, err := bucketBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		policy, err := readPolicyArg(args[1], c.InOrStdin())
		if err != nil {
			return err
		}
		return bucketadmin.RunPolicySet(c.Context(), bucketadmin.PolicySetOptions{
			BaseOptions: base, Bucket: args[0], Policy: policy,
		})
	},
}

var bucketPolicyDeleteCmd = &cobra.Command{
	Use:     "delete <bucket>",
	Short:   "Remove the bucket policy",
	Args:    cobra.ExactArgs(1),
	Example: `  grainfs bucket policy delete my-bucket`,
	RunE: func(c *cobra.Command, args []string) error {
		base, err := bucketBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return bucketadmin.RunPolicyDelete(c.Context(), bucketadmin.PolicyDeleteOptions{
			BaseOptions: base, Bucket: args[0],
		})
	},
}

// readPolicyArg reads policy JSON from a file path or stdin ("-").
// The bytes are sent verbatim to bucketadmin; the server is the
// authority on validation.
func readPolicyArg(arg string, stdin io.Reader) ([]byte, error) {
	var (
		raw []byte
		err error
	)
	if arg == "-" {
		raw, err = io.ReadAll(stdin)
	} else {
		raw, err = os.ReadFile(arg)
	}
	if err != nil {
		return nil, fmt.Errorf("read policy: %w", err)
	}
	return raw, nil
}

func init() {
	bucketPolicyParentCmd.AddCommand(
		bucketPolicyGetCmd,
		bucketPolicySetCmd,
		bucketPolicyDeleteCmd,
	)
	bucketCmd.AddCommand(bucketPolicyParentCmd)
}
