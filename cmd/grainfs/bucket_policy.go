package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"

	"github.com/spf13/cobra"
)

func bucketPolicyCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "policy",
		Short: "Manage bucket policies",
	}
	cmd.AddCommand(bucketPolicyGetCmd(), bucketPolicySetCmd(), bucketPolicyDeleteCmd())
	return cmd
}

func bucketPolicyGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get <bucket>",
		Short: "Get the bucket policy (pretty-printed JSON)",
		Args:  cobra.ExactArgs(1),
		Example: `  grainfs bucket policy get my-bucket
  grainfs bucket --json policy get my-bucket`,
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			out, err := iamRequest(c.Context(), sock, "GET",
				"/v1/buckets/"+url.PathEscape(args[0])+"/policy", nil)
			if err != nil {
				return err
			}
			asJSON, _ := c.Flags().GetBool("json")
			if asJSON {
				fmt.Fprintln(c.OutOrStdout(), string(out))
				return nil
			}
			var resp struct {
				Policy json.RawMessage `json:"policy"`
			}
			if err := json.Unmarshal(out, &resp); err != nil {
				fmt.Fprintln(c.OutOrStdout(), string(out))
				return nil
			}
			pretty, err := json.MarshalIndent(resp.Policy, "", "  ")
			if err != nil {
				fmt.Fprintln(c.OutOrStdout(), string(resp.Policy))
				return nil
			}
			fmt.Fprintln(c.OutOrStdout(), string(pretty))
			return nil
		},
	}
}

func bucketPolicySetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "set <bucket> <policy-file|->",
		Short: `Set the bucket policy from a JSON file or stdin ("-")`,
		Args:  cobra.ExactArgs(2),
		Example: `  grainfs bucket policy set my-bucket policy.json
  cat policy.json | grainfs bucket policy set my-bucket -`,
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			policyJSON, err := readPolicyArg(args[1], c.InOrStdin())
			if err != nil {
				return err
			}
			body := map[string]json.RawMessage{"policy": policyJSON}
			_, err = iamRequest(c.Context(), sock, "PUT",
				"/v1/buckets/"+url.PathEscape(args[0])+"/policy", body)
			if err != nil {
				return err
			}
			fmt.Fprintf(c.OutOrStdout(), "Policy set for bucket %s\n", args[0])
			return nil
		},
	}
}

func bucketPolicyDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "delete <bucket>",
		Short:   "Remove the bucket policy",
		Args:    cobra.ExactArgs(1),
		Example: `  grainfs bucket policy delete my-bucket`,
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			_, err = iamRequest(c.Context(), sock, "DELETE",
				"/v1/buckets/"+url.PathEscape(args[0])+"/policy", nil)
			if err != nil {
				return err
			}
			fmt.Fprintf(c.OutOrStdout(), "Policy removed for bucket %s\n", args[0])
			return nil
		},
	}
}

// readPolicyArg reads policy JSON from a file path or stdin ("-").
func readPolicyArg(arg string, stdin io.Reader) (json.RawMessage, error) {
	var raw []byte
	var err error
	if arg == "-" {
		raw, err = io.ReadAll(stdin)
	} else {
		raw, err = os.ReadFile(arg)
	}
	if err != nil {
		return nil, fmt.Errorf("read policy: %w", err)
	}
	if !json.Valid(raw) {
		return nil, fmt.Errorf("policy is not valid JSON")
	}
	return json.RawMessage(raw), nil
}
