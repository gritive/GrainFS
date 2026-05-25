package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/clusteradmin"
)

func init() {
	clusterCmd.AddCommand(clusterRotateKeyCmd())
}

func clusterRotateKeyCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "rotate-key",
		Short: "Rolling cluster-key rotation (zero-downtime)",
		Long: `Cluster-key rotation tooling. Operations:

  begin   — propose a new PSK for the cluster (leader only)
  status  — print the current rotation state
  abort   — cancel an in-flight rotation (leader only)

Communication uses a Unix domain socket bound by the running server with mode
0600, so only the operator on the local node can issue rotation commands. Pass
the socket path via --endpoint (e.g. --endpoint ./tmp/rotate.sock); the server
logs the path on startup ("rotate endpoint"). The rotate-key socket is
distinct from admin.sock so PSK-bearing operations stay owner-only.

Workflow (online rolling, zero-downtime):
  1. Generate or supply a new 32-byte PSK (64 hex chars).
  2. Run 'cluster rotate-key begin --new-key=...' on the meta-raft leader.
  3. The cluster auto-progresses through rotation states:
       begun:    accept set = [OLD, NEW], present OLD
       switched: accept = [OLD, NEW], present NEW (after grace)
       steady:   accept = [NEW], OLD demoted to previous.key
  4. Operator must distribute the same new PSK to all peers' --cluster-key
     environment / config so future restarts pick it up. Workers consume
     keys.d/next.key from disk during rotation.

Failures roll back automatically:
  - abort while begun: revert to OLD
  - abort while switched: forward-roll to NEW (D18; some peers already present NEW)
  - global timeout (30 min): auto-abort`,
	}
	c.AddCommand(clusterRotateKeyBegin(), clusterRotateKeyStatus(), clusterRotateKeyAbort())
	c.PersistentFlags().String("endpoint", "",
		"rotate-key socket path (NOT admin.sock; default <data-dir>/rotate.sock); "+
			"server logs the path on startup as \"rotate endpoint\"")
	return c
}

func rotateKeyClientFromCmd(cmd *cobra.Command) (*clusteradmin.Client, error) {
	endpoint, _ := cmd.Flags().GetString("endpoint")
	if endpoint == "" {
		return nil, fmt.Errorf("rotate endpoint not configured.\n" +
			"  Hint:  grainfs cluster rotate-key <begin|status|abort> --endpoint <data-dir>/rotate.sock")
	}
	// rotate-key shares clusteradmin's UDS dispatch but talks to its own
	// 0600-mode socket. http(s):// rejection isn't applied here because
	// rotate-key has never accepted HTTP URLs in the field.
	return clusteradmin.NewClient(endpoint), nil
}

func clusterRotateKeyBegin() *cobra.Command {
	c := &cobra.Command{
		Use:   "begin",
		Short: "Start a new rotation with the supplied PSK",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := rotateKeyClientFromCmd(cmd)
			if err != nil {
				return err
			}
			newKey, _ := cmd.Flags().GetString("new-key")
			if newKey == "" {
				gen, _ := cmd.Flags().GetBool("generate")
				if !gen {
					return fmt.Errorf("either --new-key=<64-hex> or --generate is required")
				}
				buf := make([]byte, 32)
				if _, err := rand.Read(buf); err != nil {
					return fmt.Errorf("rand: %w", err)
				}
				newKey = hex.EncodeToString(buf)
				fmt.Fprintf(cmd.OutOrStdout(), "Generated new PSK: %s\n", newKey)
				fmt.Fprintln(cmd.OutOrStdout(), "Save this securely — you must distribute it to all peers' configs.")
			}
			resp, err := client.RotateKeyBegin(cmd.Context(), newKey)
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return fmt.Errorf("server: %s", resp.Error)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Rotation started: state=%s rotation_id=%s\n", resp.State, resp.RotationID)
			fmt.Fprintf(cmd.OutOrStdout(), "  OLD SPKI: %s\n", resp.OldSPKI)
			fmt.Fprintf(cmd.OutOrStdout(), "  NEW SPKI: %s\n", resp.NewSPKI)
			fmt.Fprintln(cmd.OutOrStdout(), "  Cluster will auto-progress. Watch with 'cluster rotate-key status'.")
			return nil
		},
	}
	c.Flags().String("new-key", "", "new cluster PSK (64 hex chars / 32 bytes); mutually exclusive with --generate")
	c.Flags().Bool("generate", false, "auto-generate a new 32-byte PSK")
	return c
}

func clusterRotateKeyStatus() *cobra.Command {
	c := &cobra.Command{
		Use:   "status",
		Short: "Show current rotation state",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := rotateKeyClientFromCmd(cmd)
			if err != nil {
				return err
			}
			resp, err := client.RotateKeyStatus(cmd.Context())
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return fmt.Errorf("server: %s", resp.Error)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "state: %s\n", resp.State)
			if resp.RotationID != "" {
				fmt.Fprintf(cmd.OutOrStdout(), "rotation_id: %s\n", resp.RotationID)
			}
			if resp.OldSPKI != "" {
				fmt.Fprintf(cmd.OutOrStdout(), "old_spki: %s\n", resp.OldSPKI)
			}
			if resp.NewSPKI != "" {
				fmt.Fprintf(cmd.OutOrStdout(), "new_spki: %s\n", resp.NewSPKI)
			}
			return nil
		},
	}
	return c
}

func clusterRotateKeyAbort() *cobra.Command {
	c := &cobra.Command{
		Use:   "abort",
		Short: "Cancel an in-flight rotation",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := rotateKeyClientFromCmd(cmd)
			if err != nil {
				return err
			}
			reason, _ := cmd.Flags().GetString("reason")
			resp, err := client.RotateKeyAbort(cmd.Context(), reason)
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return fmt.Errorf("server: %s", resp.Error)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Rotation aborted. State now: %s\n", resp.State)
			return nil
		},
	}
	c.Flags().String("reason", "operator", "abort reason (logged in raft entry)")
	return c
}
