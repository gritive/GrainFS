package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/adminapi"
)

var joinCmd = &cobra.Command{
	Use:   "join <peer-raft-addr>",
	Short: "Join this node to an existing GrainFS cluster (runtime, via admin socket)",
	Long: `Send a join request to the running grainfs serve process via its admin UDS.

If the node is already a cluster member the call is a no-op.
If the node is a solo bootstrap and the peer is reachable, the server will
restart and join the cluster on next boot.

Example:
  grainfs join 192.168.1.10:8301 --endpoint /data/admin.sock`,
	Args: cobra.ExactArgs(1),
	RunE: runJoin,
}

func runJoin(cmd *cobra.Command, args []string) error {
	peerAddr := args[0]
	force, _ := cmd.Flags().GetBool("force")
	client, err := adminClientFromCmd(cmd)
	if err != nil {
		return err
	}
	ctx, cancel := applyAdminTimeout(context.Background(), cmd)
	defer cancel()

	req := struct {
		PeerAddr string `json:"peer_addr"`
		Force    bool   `json:"force,omitempty"`
	}{PeerAddr: peerAddr, Force: force}
	var resp struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	}
	if err := client.Post(ctx, "/v1/cluster/join", req, &resp); err != nil {
		var apiErr *adminapi.Error
		if errors.As(err, &apiErr) && apiErr.Status == 409 {
			var body struct {
				Status  string `json:"status"`
				Message string `json:"message"`
			}
			if json.Unmarshal([]byte(apiErr.Message), &body) == nil {
				fmt.Fprintf(cmd.OutOrStdout(), "status: %s\n", body.Status)
				if body.Message != "" {
					fmt.Fprintf(cmd.OutOrStdout(), "message: %s\n", body.Message)
				}
				return fmt.Errorf("%s", body.Status)
			}
		}
		return fmt.Errorf("join request: %w", err)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "status: %s\n", resp.Status)
	if resp.Message != "" {
		fmt.Fprintf(cmd.OutOrStdout(), "message: %s\n", resp.Message)
	}
	return nil
}

func init() {
	registerAdminEndpointFlag(joinCmd)
	registerAdminTimeoutFlag(joinCmd)
	joinCmd.Flags().Bool("force", false, "force join even if solo node has user data (data will be discarded)")
	rootCmd.AddCommand(joinCmd)
}
