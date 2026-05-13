package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"text/tabwriter"
	"time"

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
		Example: `  grainfs bucket upstream put my-bucket --upstream-url http://minio:9000 \
      --access-key AKID --secret-key-stdin
  echo "$SK" | grainfs bucket upstream put my-bucket --upstream-url http://minio:9000 \
      --access-key AKID --secret-key-stdin`,
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
			_, err = iamRequest(c.Context(), sock, "PUT", "/v1/upstreams", body)
			if err != nil {
				return err
			}
			fmt.Fprintf(c.OutOrStdout(), "Upstream configured for bucket %s\n", args[0])
			return nil
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
		Example: `  grainfs bucket upstream get my-bucket
  grainfs bucket --json upstream get my-bucket`,
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
			asJSON, _ := c.Flags().GetBool("json")
			if asJSON {
				fmt.Fprintln(c.OutOrStdout(), string(out))
				return nil
			}
			var item struct {
				Bucket      string    `json:"bucket"`
				UpstreamURL string    `json:"upstream_url"`
				AccessKey   string    `json:"access_key"`
				CreatedAt   time.Time `json:"created_at"`
				CreatedBy   string    `json:"created_by"`
			}
			if err := json.Unmarshal(out, &item); err != nil {
				return fmt.Errorf("parse response: %w", err)
			}
			tw := tabwriter.NewWriter(c.OutOrStdout(), 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "BUCKET\tURL\tACCESS KEY\tCREATED AT")
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
				item.Bucket, item.UpstreamURL, item.AccessKey, item.CreatedAt.Format(time.RFC3339))
			return tw.Flush()
		},
	}

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List all bucket-upstream configurations",
		Example: `  grainfs bucket upstream list
  grainfs bucket --json upstream list`,
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			out, err := iamRequest(c.Context(), sock, "GET", "/v1/upstreams", nil)
			if err != nil {
				return err
			}
			asJSON, _ := c.Flags().GetBool("json")
			if asJSON {
				fmt.Fprintln(c.OutOrStdout(), string(out))
				return nil
			}
			var resp struct {
				Upstreams []struct {
					Bucket      string    `json:"bucket"`
					UpstreamURL string    `json:"upstream_url"`
					AccessKey   string    `json:"access_key"`
					CreatedAt   time.Time `json:"created_at"`
				} `json:"upstreams"`
			}
			if err := json.Unmarshal(out, &resp); err != nil {
				return fmt.Errorf("parse response: %w", err)
			}
			tw := tabwriter.NewWriter(c.OutOrStdout(), 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "BUCKET\tURL\tACCESS KEY\tCREATED AT")
			for _, u := range resp.Upstreams {
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
					u.Bucket, u.UpstreamURL, u.AccessKey, u.CreatedAt.Format(time.RFC3339))
			}
			return tw.Flush()
		},
	}

	deleteCmd := &cobra.Command{
		Use:     "delete <bucket>",
		Short:   "Remove the upstream config for a bucket",
		Args:    cobra.ExactArgs(1),
		Example: `  grainfs bucket upstream delete my-bucket`,
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			_, err = iamRequest(c.Context(), sock, "DELETE",
				"/v1/buckets/"+url.PathEscape(args[0])+"/upstream", nil)
			if err != nil {
				return err
			}
			fmt.Fprintf(c.OutOrStdout(), "Removed upstream config for bucket %s\n", args[0])
			return nil
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
