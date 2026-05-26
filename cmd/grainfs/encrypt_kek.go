package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/clusteradmin"
)

var encryptKEKCmd = &cobra.Command{
	Use:   "kek",
	Short: "Manage cluster-wide KEK (key encryption key) lifecycle (admin only)",
}

// --- rotate ---

var rotateIKnow bool

var encryptKEKRotateCmd = &cobra.Command{
	Use:   "rotate",
	Short: "Rotate cluster KEK to next version. Re-wraps all live DEKs deterministically.",
	Long: `Rotate the active KEK to version active+1.

This command is admin-only and reaches the cluster via the local admin UDS.
It triggers a raft-replicated KEK rotation: leader generates K_new, re-wraps
every live DEK under K_new, and propagates as a single raft command. All
followers Apply the pre-computed re-wraps verbatim (no node-side re-seal).

WARNING: rotation is irreversible. The new KEK becomes active immediately;
old KEKs remain in the keystore but stop being used for new seals. Use
'encrypt kek retire' + 'encrypt kek prune' to permanently delete an old KEK
after lease drain.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if !rotateIKnow {
			return errors.New("rotation is irreversible. pass --i-know to confirm")
		}
		ep, err := adminEndpointFromCmd(cmd)
		if err != nil {
			return err
		}
		return clusteradmin.NewClient(ep).EncryptKEKRotate(cmd.Context())
	},
}

// --- retire ---

var (
	retireVersion     uint32
	retireConfirm     string
	retireClusterName string
)

var encryptKEKRetireCmd = &cobra.Command{
	Use:   "retire",
	Short: "Mark KEK version as retiring (Phase 1 of permanent removal)",
	Long: `Mark a KEK version as retiring to begin Phase 1 of permanent removal.

After retire, DEK leases held under this KEK version must drain before
the version can be pruned. Use 'encrypt kek status' to monitor lease counts.
Follow with 'encrypt kek prune' once all leases are zero.

The --cluster-name flag must match the node_id shown by 'grainfs cluster status'.
This is a second-factor safety check to prevent accidental cross-cluster operations.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if retireVersion == 0 {
			return fmt.Errorf("version must be >= 1; version 0 is reserved")
		}
		expectedConfirm := fmt.Sprintf("delete-permanently-%d", retireVersion)
		if retireConfirm != expectedConfirm {
			return fmt.Errorf("--confirm-name must be %q (got %q)", expectedConfirm, retireConfirm)
		}
		ep, err := adminEndpointFromCmd(cmd)
		if err != nil {
			return err
		}
		client := clusteradmin.NewClient(ep)
		actualName, err := kekClusterNodeID(cmd.Context(), client)
		if err != nil {
			return fmt.Errorf("retire: lookup cluster name: %w", err)
		}
		if retireClusterName != actualName {
			return fmt.Errorf("--cluster-name must match this cluster's name %q (got %q)", actualName, retireClusterName)
		}
		return client.EncryptKEKRetire(cmd.Context(), retireVersion, retireConfirm)
	},
}

// --- prune ---

var (
	pruneVersion     uint32
	pruneConfirm     string
	pruneClusterName string
)

var encryptKEKPruneCmd = &cobra.Command{
	Use:   "prune",
	Short: "Permanently delete a retired KEK version (Phase 2 of removal)",
	Long: `Permanently delete a KEK version that has been retired and fully drained.

All nodes must attest zero leases under the retiring version before the
leader accepts the prune command. The prune is voter-stamped: the leader
collects lease attestations from all voters as part of the raft proposal.

The --cluster-name flag must match the node_id shown by 'grainfs cluster status'.
This is a second-factor safety check to prevent accidental cross-cluster operations.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if pruneVersion == 0 {
			return fmt.Errorf("version must be >= 1; version 0 is reserved")
		}
		expectedConfirm := fmt.Sprintf("delete-permanently-%d", pruneVersion)
		if pruneConfirm != expectedConfirm {
			return fmt.Errorf("--confirm-name must be %q (got %q)", expectedConfirm, pruneConfirm)
		}
		ep, err := adminEndpointFromCmd(cmd)
		if err != nil {
			return err
		}
		client := clusteradmin.NewClient(ep)
		actualName, err := kekClusterNodeID(cmd.Context(), client)
		if err != nil {
			return fmt.Errorf("prune: lookup cluster name: %w", err)
		}
		if pruneClusterName != actualName {
			return fmt.Errorf("--cluster-name must match this cluster's name %q (got %q)", actualName, pruneClusterName)
		}
		return client.EncryptKEKPrune(cmd.Context(), pruneVersion, pruneConfirm)
	},
}

// --- status ---

var encryptKEKStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show KEK lifecycle status (active version, retained versions, per-version state)",
	RunE: func(cmd *cobra.Command, args []string) error {
		ep, err := adminEndpointFromCmd(cmd)
		if err != nil {
			return err
		}
		s, err := clusteradmin.NewClient(ep).EncryptKEKStatus(cmd.Context())
		if err != nil {
			return err
		}
		if jsonOut(cmd) {
			return printJSON(cmd, s)
		}
		printKEKStatus(cmd.OutOrStdout(), s)
		return nil
	},
}

// kekClusterNodeID fetches GET /v1/cluster/status and returns NodeID, which
// serves as the cluster identity for --cluster-name verification.
func kekClusterNodeID(ctx context.Context, client *clusteradmin.Client) (string, error) {
	s, err := client.Status(ctx)
	if err != nil {
		return "", err
	}
	return s.NodeID, nil
}

func printKEKStatus(w io.Writer, s *clusteradmin.KEKStatus) {
	fmt.Fprintf(w, "active_version: %d\n", s.ActiveVersion)
	if len(s.Versions) == 0 {
		fmt.Fprintln(w, "versions: (none)")
		return
	}
	fmt.Fprintln(w, "versions:")
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	for _, v := range s.Versions {
		active := ""
		if v.Version == s.ActiveVersion {
			active = " (active)"
		}
		fmt.Fprintf(tw, "  version=%d\tstatus=%s%s\n", v.Version, v.Status, active)
	}
	_ = tw.Flush()
}

func init() {
	encryptKEKCmd.PersistentFlags().String("endpoint", "",
		"admin Unix socket path (overrides GRAINFS_ADMIN_SOCKET env var)")

	encryptKEKRotateCmd.Flags().BoolVar(&rotateIKnow, "i-know", false,
		"confirm that rotation is irreversible")

	encryptKEKRetireCmd.Flags().Uint32Var(&retireVersion, "version", 0,
		"KEK version to retire")
	encryptKEKRetireCmd.Flags().StringVar(&retireConfirm, "confirm-name", "",
		`confirm string: must be "delete-permanently-<version>"`)
	encryptKEKRetireCmd.Flags().StringVar(&retireClusterName, "cluster-name", "",
		"node_id of this cluster as shown by 'grainfs cluster status' (second-factor check)")
	_ = encryptKEKRetireCmd.MarkFlagRequired("version")
	_ = encryptKEKRetireCmd.MarkFlagRequired("confirm-name")
	_ = encryptKEKRetireCmd.MarkFlagRequired("cluster-name")

	encryptKEKPruneCmd.Flags().Uint32Var(&pruneVersion, "version", 0,
		"KEK version to prune")
	encryptKEKPruneCmd.Flags().StringVar(&pruneConfirm, "confirm-name", "",
		`confirm string: must be "delete-permanently-<version>"`)
	encryptKEKPruneCmd.Flags().StringVar(&pruneClusterName, "cluster-name", "",
		"node_id of this cluster as shown by 'grainfs cluster status' (second-factor check)")
	_ = encryptKEKPruneCmd.MarkFlagRequired("version")
	_ = encryptKEKPruneCmd.MarkFlagRequired("confirm-name")
	_ = encryptKEKPruneCmd.MarkFlagRequired("cluster-name")

	encryptKEKStatusCmd.Flags().String("format", "text", "output format: text or json")

	encryptKEKCmd.AddCommand(
		encryptKEKRotateCmd,
		encryptKEKRetireCmd,
		encryptKEKPruneCmd,
		encryptKEKStatusCmd,
	)
	encryptCmd.AddCommand(encryptKEKCmd)
}
