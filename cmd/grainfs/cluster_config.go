package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
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
