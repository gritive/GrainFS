package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

var iamCmd = &cobra.Command{
	Use:   "iam",
	Short: "Manage GrainFS IAM (ServiceAccounts, AccessKeys, Grants)",
}

// iamHTTPClient builds an http.Client that dials the admin UDS.
func iamHTTPClient(sock string) *http.Client {
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", sock)
			},
		},
	}
}

// iamRequest sends method+path+body to the admin UDS at sock and returns body bytes.
// Non-2xx -> error including server message.
func iamRequest(ctx context.Context, sock, method, path string, body any) ([]byte, error) {
	var rdr io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal body: %w", err)
		}
		rdr = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://unix"+path, rdr)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := iamHTTPClient(sock).Do(req)
	if err != nil {
		return nil, fmt.Errorf("admin UDS dial %s: %w", sock, err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("admin %s %s -> %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(out)))
	}
	return out, nil
}

func iamSACmd() *cobra.Command {
	cmd := &cobra.Command{Use: "sa", Short: "Manage ServiceAccounts"}

	createCmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a ServiceAccount; returns the first AccessKey + one-time secret",
		Example: `  grainfs iam sa create alice --description "data team"
  grainfs iam --json sa create alice`,
		Args: cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			desc, _ := c.Flags().GetString("description")
			out, err := iamRequest(c.Context(), sock, "POST", "/v1/iam/sa",
				map[string]string{"name": args[0], "description": desc})
			if err != nil {
				return err
			}
			asJSON, _ := c.Flags().GetBool("json")
			if asJSON {
				fmt.Fprintln(c.OutOrStdout(), string(out))
				return nil
			}
			var resp struct {
				SAID      string `json:"sa_id"`
				Name      string `json:"name"`
				AccessKey string `json:"access_key"`
				SecretKey string `json:"secret_key"`
			}
			if err := json.Unmarshal(out, &resp); err != nil {
				return fmt.Errorf("parse response: %w", err)
			}
			w := c.OutOrStdout()
			fmt.Fprintf(w, "Created service account %s\n", resp.Name)
			tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
			fmt.Fprintf(tw, "sa_id:\t%s\n", resp.SAID)
			fmt.Fprintf(tw, "access_key:\t%s\n", resp.AccessKey)
			fmt.Fprintf(tw, "secret_key:\t%s\n", resp.SecretKey)
			if err := tw.Flush(); err != nil {
				return err
			}
			fmt.Fprintln(w, "Store the secret_key now — it will not be shown again.")
			return nil
		},
	}
	createCmd.Flags().String("description", "", "free-form SA description")

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List ServiceAccounts",
		Example: `  grainfs iam sa list
  grainfs iam --json sa list`,
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			out, err := iamRequest(c.Context(), sock, "GET", "/v1/iam/sa", nil)
			if err != nil {
				return err
			}
			asJSON, _ := c.Flags().GetBool("json")
			if asJSON {
				fmt.Fprintln(c.OutOrStdout(), string(out))
				return nil
			}
			var items []struct {
				SAID        string    `json:"sa_id"`
				Name        string    `json:"name"`
				Description string    `json:"description"`
				CreatedAt   time.Time `json:"created_at"`
				NumKeys     int       `json:"num_keys"`
				NumGrants   int       `json:"num_grants"`
			}
			if err := json.Unmarshal(out, &items); err != nil {
				return fmt.Errorf("parse response: %w", err)
			}
			tw := tabwriter.NewWriter(c.OutOrStdout(), 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "SA ID\tNAME\tKEYS\tGRANTS\tCREATED AT")
			for _, sa := range items {
				fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%s\n",
					sa.SAID, sa.Name, sa.NumKeys, sa.NumGrants,
					sa.CreatedAt.Format(time.RFC3339))
			}
			return tw.Flush()
		},
	}

	getCmd := &cobra.Command{
		Use:   "get <sa_id>",
		Short: "Show SA detail",
		Example: `  grainfs iam sa get sa-abc123
  grainfs iam --json sa get sa-abc123`,
		Args: cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			out, err := iamRequest(c.Context(), sock, "GET", "/v1/iam/sa/"+url.PathEscape(args[0]), nil)
			if err != nil {
				return err
			}
			asJSON, _ := c.Flags().GetBool("json")
			if asJSON {
				fmt.Fprintln(c.OutOrStdout(), string(out))
				return nil
			}
			var item struct {
				SAID        string    `json:"sa_id"`
				Name        string    `json:"name"`
				Description string    `json:"description"`
				CreatedAt   time.Time `json:"created_at"`
				CreatedBy   string    `json:"created_by"`
			}
			if err := json.Unmarshal(out, &item); err != nil {
				return fmt.Errorf("parse response: %w", err)
			}
			tw := tabwriter.NewWriter(c.OutOrStdout(), 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "SA ID\tNAME\tDESCRIPTION\tCREATED AT")
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
				item.SAID, item.Name, item.Description, item.CreatedAt.Format(time.RFC3339))
			return tw.Flush()
		},
	}

	deleteCmd := &cobra.Command{
		Use:     "delete <sa_id>",
		Short:   "Delete an SA (cascades to its keys + grants via FSM)",
		Example: `  grainfs iam sa delete sa-abc123`,
		Args:    cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			_, err = iamRequest(c.Context(), sock, "DELETE", "/v1/iam/sa/"+url.PathEscape(args[0]), nil)
			if err != nil {
				return err
			}
			fmt.Fprintf(c.OutOrStdout(), "Deleted service account %s\n", args[0])
			return nil
		},
	}

	cmd.AddCommand(createCmd, listCmd, getCmd, deleteCmd)
	return cmd
}

func iamKeyCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "key", Short: "Manage SA AccessKeys"}

	createCmd := &cobra.Command{
		Use:   "create <sa_id>",
		Short: "Issue a new AccessKey for the SA (one-time secret_key in response)",
		Example: `  grainfs iam key create sa-abc123
  grainfs iam key create sa-abc123 --bucket logs --bucket reports`,
		Args: cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			buckets, _ := c.Flags().GetStringSlice("bucket")
			body := map[string]any{}
			if len(buckets) > 0 {
				body["buckets"] = buckets
			}
			out, err := iamRequest(c.Context(), sock, "POST",
				"/v1/iam/sa/"+url.PathEscape(args[0])+"/key", body)
			if err != nil {
				return err
			}
			fmt.Fprintln(c.OutOrStdout(), string(out))
			return nil
		},
	}
	createCmd.Flags().StringSlice("bucket", nil,
		"restrict the new key to specific buckets (repeatable; default: unrestricted)")

	cmd.AddCommand(
		createCmd,
		&cobra.Command{
			Use:     "revoke <sa_id> <access_key>",
			Short:   "Revoke an AccessKey",
			Example: `  grainfs iam key revoke sa-abc123 AK1234`,
			Args:    cobra.ExactArgs(2),
			RunE: func(c *cobra.Command, args []string) error {
				sock, err := adminEndpointFromCmd(c)
				if err != nil {
					return err
				}
				_, err = iamRequest(c.Context(), sock, "DELETE",
					"/v1/iam/sa/"+url.PathEscape(args[0])+"/key/"+url.PathEscape(args[1]), nil)
				return err
			},
		},
	)
	return cmd
}

func iamGrantCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "grant", Short: "Manage Grants"}

	putCmd := &cobra.Command{
		Use:     "put <sa_id> <bucket> <role>",
		Short:   "Grant role on bucket to SA (role: Read|Write|Admin)",
		Example: `  grainfs iam grant put sa-abc123 my-bucket Write`,
		Args:    cobra.ExactArgs(3),
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			_, err = iamRequest(c.Context(), sock, "PUT", "/v1/iam/grant",
				map[string]string{"sa_id": args[0], "bucket": args[1], "role": args[2]})
			return err
		},
	}

	delCmd := &cobra.Command{
		Use:     "delete <sa_id> <bucket>",
		Short:   "Remove grant from SA on bucket",
		Example: `  grainfs iam grant delete sa-abc123 my-bucket`,
		Args:    cobra.ExactArgs(2),
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			_, err = iamRequest(c.Context(), sock, "DELETE", "/v1/iam/grant",
				map[string]string{"sa_id": args[0], "bucket": args[1]})
			return err
		},
	}

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List grants (filter with --sa or --bucket)",
		Example: `  grainfs iam grant list
  grainfs iam grant list --sa sa-abc123
  grainfs iam grant list --bucket my-bucket`,
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			sa, _ := c.Flags().GetString("sa")
			bucket, _ := c.Flags().GetString("bucket")
			path := "/v1/iam/grant"
			q := url.Values{}
			if sa != "" {
				q.Set("sa", sa)
			}
			if bucket != "" {
				q.Set("bucket", bucket)
			}
			if encoded := q.Encode(); encoded != "" {
				path += "?" + encoded
			}
			out, err := iamRequest(c.Context(), sock, "GET", path, nil)
			if err != nil {
				return err
			}
			fmt.Fprintln(c.OutOrStdout(), string(out))
			return nil
		},
	}
	listCmd.Flags().String("sa", "", "filter by sa_id")
	listCmd.Flags().String("bucket", "", "filter by bucket")

	cmd.AddCommand(putCmd, delCmd, listCmd)
	return cmd
}

func init() {
	iamCmd.PersistentFlags().String("endpoint", "",
		"admin Unix socket path (overrides GRAINFS_ADMIN_SOCKET env var)")
	iamCmd.PersistentFlags().Bool("json", false, "output raw JSON")
	iamCmd.AddCommand(iamSACmd(), iamKeyCmd(), iamGrantCmd())
	rootCmd.AddCommand(iamCmd)
}
