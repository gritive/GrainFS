package main

import (
	"bufio"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

func bucketUpstreamCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "upstream",
		Short: "Manage per-bucket pull-through upstream credentials",
	}

	putCmd := &cobra.Command{
		Use:   "put <bucket>",
		Short: "Register or rotate the upstream credentials for a bucket",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			upstreamURL, _ := c.Flags().GetString("upstream-url")
			ak, _ := c.Flags().GetString("access-key")
			useStdin, _ := c.Flags().GetBool("secret-key-stdin")
			skFile, _ := c.Flags().GetString("secret-key-file")

			if strings.TrimSpace(upstreamURL) == "" {
				return fmt.Errorf("--upstream-url is required")
			}
			if strings.TrimSpace(ak) == "" {
				return fmt.Errorf("--access-key is required")
			}
			if useStdin == (skFile != "") {
				return fmt.Errorf("exactly one of --secret-key-stdin or --secret-key-file=<path> is required")
			}
			sk, err := readSecretKey(useStdin, skFile, c.InOrStdin())
			if err != nil {
				return err
			}
			if sk == "" {
				return fmt.Errorf("secret key is empty")
			}
			body := map[string]string{
				"bucket":       args[0],
				"upstream_url": upstreamURL,
				"access_key":   ak,
				"secret_key":   sk,
			}
			_, err = iamRequest(c.Context(), sock, "PUT", "/v1/buckets/upstream", body)
			return err
		},
	}
	putCmd.Flags().String("upstream-url", "", "upstream S3 endpoint URL (e.g., http://minio:9000)")
	putCmd.Flags().String("access-key", "", "upstream access key")
	putCmd.Flags().Bool("secret-key-stdin", false, "read upstream secret key from stdin (one line, trailing newline trimmed)")
	putCmd.Flags().String("secret-key-file", "", "read upstream secret key from file (whitespace-trimmed)")

	getCmd := &cobra.Command{
		Use:   "get <bucket>",
		Short: "Show the upstream config for a bucket (secret_key never returned)",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			out, err := iamRequest(c.Context(), sock, "GET",
				"/v1/buckets/"+url.PathEscape(args[0])+"/upstream", nil)
			if err != nil {
				return err
			}
			fmt.Fprintln(c.OutOrStdout(), string(out))
			return nil
		},
	}

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List all bucket-upstream configurations",
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			out, err := iamRequest(c.Context(), sock, "GET", "/v1/buckets/upstream", nil)
			if err != nil {
				return err
			}
			fmt.Fprintln(c.OutOrStdout(), string(out))
			return nil
		},
	}

	deleteCmd := &cobra.Command{
		Use:   "delete <bucket>",
		Short: "Remove the upstream config for a bucket",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			_, err = iamRequest(c.Context(), sock, "DELETE",
				"/v1/buckets/"+url.PathEscape(args[0])+"/upstream", nil)
			return err
		},
	}

	cmd.AddCommand(putCmd, getCmd, listCmd, deleteCmd)
	return cmd
}

// readSecretKey returns the upstream secret key from stdin (one line) or
// from the given file (whitespace-trimmed). Caller guarantees exactly one
// source is non-empty.
func readSecretKey(useStdin bool, file string, stdin io.Reader) (string, error) {
	if useStdin {
		s := bufio.NewScanner(stdin)
		if !s.Scan() {
			if err := s.Err(); err != nil {
				return "", fmt.Errorf("read stdin: %w", err)
			}
			return "", fmt.Errorf("stdin closed before secret key was read")
		}
		return strings.TrimRight(s.Text(), "\r\n"), nil
	}
	buf, err := os.ReadFile(file)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", file, err)
	}
	return strings.TrimSpace(string(buf)), nil
}
