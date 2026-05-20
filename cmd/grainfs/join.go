package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/clusteradmin"
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
	force, _ := cmd.Flags().GetBool("force")
	ep, err := adminEndpointFromCmd(cmd)
	if err != nil {
		return err
	}
	ctx, cancel := applyAdminTimeout(context.Background(), cmd)
	defer cancel()
	resp, err := clusteradmin.NewClient(ep).JoinViaUDS(ctx, args[0], force)
	status, message := "", ""
	var conflict *clusteradmin.JoinConflictError
	switch {
	case errors.As(err, &conflict):
		status, message = conflict.Status, conflict.Message
	case err != nil:
		return err
	default:
		status, message = resp.Status, resp.Message
	}
	fmt.Fprintf(cmd.OutOrStdout(), "status: %s\n", status)
	if message != "" {
		fmt.Fprintf(cmd.OutOrStdout(), "message: %s\n", message)
	}
	if conflict != nil {
		return fmt.Errorf("%s", conflict.Status)
	}
	return nil
}

func init() {
	registerAdminEndpointFlag(joinCmd)
	registerAdminTimeoutFlag(joinCmd)
	joinCmd.Flags().Bool("force", false, "force join even if solo node has user data (data will be discarded)")
	rootCmd.AddCommand(joinCmd)
}
