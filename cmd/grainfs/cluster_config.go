package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

// clusterConfigCmd is the parent of the `cluster config` read/write subcommands.
// Reads (show / get / diff) GET /v1/cluster/config over the admin UDS and render
// the effective+source view returned by the Task 9 handler. Writes (set / reset)
// are added in Task 13.
var clusterConfigCmd = &cobra.Command{
	Use:   "config",
	Short: "Inspect or change cluster-wide policy",
}

var clusterConfigShowCmd = &cobra.Command{
	Use:   "show",
	Short: "Print all cluster config values (rev + effective + source)",
	RunE: func(cmd *cobra.Command, args []string) error {
		endpoint, err := clusterEndpointFromCmd(cmd)
		if err != nil {
			return err
		}
		asJSON, _ := cmd.Flags().GetBool("json")
		return runClusterConfigShow(endpoint, asJSON, cmd.OutOrStdout())
	},
}

var clusterConfigGetCmd = &cobra.Command{
	Use:   "get <key>",
	Args:  cobra.ExactArgs(1),
	Short: "Print one cluster config value",
	RunE: func(cmd *cobra.Command, args []string) error {
		endpoint, err := clusterEndpointFromCmd(cmd)
		if err != nil {
			return err
		}
		asJSON, _ := cmd.Flags().GetBool("json")
		return runClusterConfigGet(endpoint, args[0], asJSON, cmd.OutOrStdout())
	},
}

var clusterConfigDiffCmd = &cobra.Command{
	Use:   "diff",
	Short: "Show only explicit overrides (vs code defaults)",
	RunE: func(cmd *cobra.Command, args []string) error {
		endpoint, err := clusterEndpointFromCmd(cmd)
		if err != nil {
			return err
		}
		return runClusterConfigDiff(endpoint, cmd.OutOrStdout())
	},
}

// runClusterConfigShow GETs /v1/cluster/config over UDS and prints either the
// raw JSON body (--json) or a kebab-keyed REV + KEY/EFFECTIVE/SOURCE table.
func runClusterConfigShow(socket string, asJSON bool, w io.Writer) error {
	body, err := udsGet(socket, "/v1/cluster/config")
	if err != nil {
		return err
	}
	if asJSON {
		_, err = w.Write(body)
		return err
	}
	var resp struct {
		Rev       uint64            `json:"rev"`
		Effective map[string]any    `json:"effective"`
		Source    map[string]string `json:"source"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return err
	}

	fmt.Fprintf(w, "REV: %d\n\n", resp.Rev)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "KEY\tEFFECTIVE\tSOURCE")
	keys := make([]string, 0, len(resp.Source))
	for k := range resp.Source {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(tw, "%s\t%v\t%s\n", k, resp.Effective[k], resp.Source[k])
	}
	return tw.Flush()
}

// runClusterConfigGet prints a single key.
func runClusterConfigGet(socket, key string, asJSON bool, w io.Writer) error {
	body, err := udsGet(socket, "/v1/cluster/config")
	if err != nil {
		return err
	}
	var resp struct {
		Effective map[string]any    `json:"effective"`
		Source    map[string]string `json:"source"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return err
	}
	if asJSON {
		out, _ := json.Marshal(map[string]any{
			"key":    key,
			"value":  resp.Effective[key],
			"source": resp.Source[key],
		})
		_, err := w.Write(out)
		return err
	}
	fmt.Fprintf(w, "%s = %v  (%s)\n", key, resp.Effective[key], resp.Source[key])
	return nil
}

// runClusterConfigDiff prints only rows with source == "explicit".
func runClusterConfigDiff(socket string, w io.Writer) error {
	body, err := udsGet(socket, "/v1/cluster/config")
	if err != nil {
		return err
	}
	var resp struct {
		Effective map[string]any    `json:"effective"`
		Source    map[string]string `json:"source"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "KEY\tVALUE")
	keys := make([]string, 0)
	for k, src := range resp.Source {
		if src == "explicit" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(tw, "%s\t%v\n", k, resp.Effective[k])
	}
	return tw.Flush()
}

var clusterConfigSetCmd = &cobra.Command{
	Use:   "set <key>=<value> [<key>=<value> ...]",
	Args:  cobra.MinimumNArgs(1),
	Short: "Set one or more cluster config values (atomic single PATCH)",
	RunE: func(cmd *cobra.Command, args []string) error {
		endpoint, err := clusterEndpointFromCmd(cmd)
		if err != nil {
			return err
		}
		rev, _ := cmd.Flags().GetUint64("if-match-rev")
		return runClusterConfigSet(endpoint, args, rev)
	},
}

var clusterConfigResetCmd = &cobra.Command{
	Use:   "reset <key> [<key> ...]",
	Args:  cobra.MinimumNArgs(1),
	Short: "Reset one or more keys to defaults",
	RunE: func(cmd *cobra.Command, args []string) error {
		endpoint, err := clusterEndpointFromCmd(cmd)
		if err != nil {
			return err
		}
		rev, _ := cmd.Flags().GetUint64("if-match-rev")
		return runClusterConfigReset(endpoint, args, rev)
	},
}

// runClusterConfigSet parses `<key>=<value>` pairs (value is JSON literal, with
// plain-string fallback for non-JSON values like https://… URLs), builds a
// single JSON body, and PATCHes /v1/cluster/config atomically.
func runClusterConfigSet(socket string, kvs []string, expectedRev uint64) error {
	body := map[string]any{}
	for _, kv := range kvs {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return fmt.Errorf("invalid <key>=<value>: %q", kv)
		}
		var parsed any
		if err := json.Unmarshal([]byte(v), &parsed); err != nil {
			// Plain string fallback for values like https://… that aren't valid JSON literals.
			parsed = v
		}
		body[k] = parsed
	}
	return udsPatch(socket, body, expectedRev)
}

// runClusterConfigReset PATCHes /v1/cluster/config with {"reset_keys":[...]}
// to revert keys to their code defaults.
func runClusterConfigReset(socket string, keys []string, expectedRev uint64) error {
	return udsPatch(socket, map[string]any{"reset_keys": keys}, expectedRev)
}

// udsPatch does a PATCH request over a Unix domain socket carrying the body as
// JSON and an optional If-Match-Rev header. On 200 the response body is printed
// to stdout (e.g. {"rev": N}).
func udsPatch(socket string, body map[string]any, expectedRev uint64) error {
	buf, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal patch body: %w", err)
	}
	cli := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		},
	}}
	req, err := http.NewRequest(http.MethodPatch, "http://unix/v1/cluster/config", bytes.NewReader(buf))
	if err != nil {
		return fmt.Errorf("new patch request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if expectedRev != 0 {
		req.Header.Set("If-Match-Rev", fmt.Sprintf("%d", expectedRev))
	}
	resp, err := cli.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("PATCH /v1/cluster/config: %d: %s", resp.StatusCode, respBody)
	}
	fmt.Printf("%s\n", respBody)
	return nil
}

// udsGet does a GET request over a Unix domain socket and returns the body
// bytes. Non-200 responses surface as an error including the body text.
func udsGet(socket, path string) ([]byte, error) {
	cli := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		},
	}}
	resp, err := cli.Get("http://unix" + path)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("admin api %s: %d: %s", path, resp.StatusCode, body)
	}
	return body, nil
}
