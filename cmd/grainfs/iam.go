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

	"github.com/spf13/cobra"
)

var iamCmd = &cobra.Command{
	Use:   "iam",
	Short: "Manage GrainFS IAM (ServiceAccounts, AccessKeys, Grants)",
}

// iamEndpointFromCmd resolves --endpoint and validates it. Mirrors
// clusterEndpointFromCmd: rejects http(s):// URLs because the IAM CLI
// talks to the admin Unix socket.
func iamEndpointFromCmd(cmd *cobra.Command) (string, error) {
	ep, _ := cmd.Flags().GetString("endpoint")
	ep = strings.TrimSpace(ep)
	if ep == "" {
		return "", fmt.Errorf("admin endpoint not configured.\n" +
			"  Hint: grainfs iam --endpoint <data-dir>/admin.sock <subcommand>")
	}
	if strings.HasPrefix(ep, "http://") || strings.HasPrefix(ep, "https://") {
		return "", fmt.Errorf("admin endpoint must be a UDS socket path; got %q.\n"+
			"  Use the admin socket: --endpoint <data-dir>/admin.sock", ep)
	}
	return strings.TrimPrefix(ep, "unix:"), nil
}

// iamHTTPClient builds an http.Client that dials the admin UDS.
func iamHTTPClient(sock string) *http.Client {
	return &http.Client{
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
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := iamEndpointFromCmd(c)
			if err != nil {
				return err
			}
			desc, _ := c.Flags().GetString("description")
			out, err := iamRequest(c.Context(), sock, "POST", "/v1/iam/sa",
				map[string]string{"name": args[0], "description": desc})
			if err != nil {
				return err
			}
			fmt.Fprintln(c.OutOrStdout(), string(out))
			return nil
		},
	}
	createCmd.Flags().String("description", "", "free-form SA description")

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List ServiceAccounts",
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := iamEndpointFromCmd(c)
			if err != nil {
				return err
			}
			out, err := iamRequest(c.Context(), sock, "GET", "/v1/iam/sa", nil)
			if err != nil {
				return err
			}
			fmt.Fprintln(c.OutOrStdout(), string(out))
			return nil
		},
	}

	getCmd := &cobra.Command{
		Use:   "get <sa_id>",
		Short: "Show SA detail",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := iamEndpointFromCmd(c)
			if err != nil {
				return err
			}
			out, err := iamRequest(c.Context(), sock, "GET", "/v1/iam/sa/"+url.PathEscape(args[0]), nil)
			if err != nil {
				return err
			}
			fmt.Fprintln(c.OutOrStdout(), string(out))
			return nil
		},
	}

	deleteCmd := &cobra.Command{
		Use:   "delete <sa_id>",
		Short: "Delete an SA (cascades to its keys + grants via FSM)",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := iamEndpointFromCmd(c)
			if err != nil {
				return err
			}
			_, err = iamRequest(c.Context(), sock, "DELETE", "/v1/iam/sa/"+url.PathEscape(args[0]), nil)
			return err
		},
	}

	cmd.AddCommand(createCmd, listCmd, getCmd, deleteCmd)
	return cmd
}

func iamKeyCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "key", Short: "Manage SA AccessKeys"}
	cmd.AddCommand(
		&cobra.Command{
			Use:   "create <sa_id>",
			Short: "Issue a new AccessKey for the SA (one-time secret_key in response)",
			Args:  cobra.ExactArgs(1),
			RunE: func(c *cobra.Command, args []string) error {
				sock, err := iamEndpointFromCmd(c)
				if err != nil {
					return err
				}
				out, err := iamRequest(c.Context(), sock, "POST",
					"/v1/iam/sa/"+url.PathEscape(args[0])+"/key", map[string]any{})
				if err != nil {
					return err
				}
				fmt.Fprintln(c.OutOrStdout(), string(out))
				return nil
			},
		},
		&cobra.Command{
			Use:   "revoke <sa_id> <access_key>",
			Short: "Revoke an AccessKey",
			Args:  cobra.ExactArgs(2),
			RunE: func(c *cobra.Command, args []string) error {
				sock, err := iamEndpointFromCmd(c)
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
		Use:   "put <sa_id> <bucket> <role>",
		Short: "Grant role on bucket to SA (role: Read|Write|Admin)",
		Args:  cobra.ExactArgs(3),
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := iamEndpointFromCmd(c)
			if err != nil {
				return err
			}
			_, err = iamRequest(c.Context(), sock, "PUT", "/v1/iam/grant",
				map[string]string{"sa_id": args[0], "bucket": args[1], "role": args[2]})
			return err
		},
	}

	delCmd := &cobra.Command{
		Use:   "delete <sa_id> <bucket>",
		Short: "Remove grant from SA on bucket",
		Args:  cobra.ExactArgs(2),
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := iamEndpointFromCmd(c)
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
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := iamEndpointFromCmd(c)
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
		"admin Unix socket path (required, e.g. ./tmp/admin.sock)")
	iamCmd.AddCommand(iamSACmd(), iamKeyCmd(), iamGrantCmd())
	rootCmd.AddCommand(iamCmd)
}
