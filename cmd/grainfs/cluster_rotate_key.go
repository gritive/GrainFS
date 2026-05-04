package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
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

Communication uses a Unix domain socket at $DATA/rotate.sock with mode 0600,
so only the operator on the local node can issue rotation commands.

Workflow (online rolling, zero-downtime):
  1. Generate or supply a new 32-byte PSK (64 hex chars).
  2. Run 'cluster rotate-key begin --new-key=...' on the meta-raft leader.
  3. The cluster auto-progresses through phases:
       phase 1 → 2 (begin):    accept set = [OLD, NEW], present OLD
       phase 2 → 3 (switch):   accept = [OLD, NEW], present NEW (after grace)
       phase 3 → steady:       accept = [NEW], OLD demoted to previous.key
  4. Operator must distribute the same new PSK to all peers' --cluster-key
     environment / config so future restarts pick it up. Workers consume
     keys.d/next.key from disk during rotation.

Failures roll back automatically:
  - phase 2 abort: revert to OLD
  - phase 3 abort: forward-roll to NEW (D18; some peers already present NEW)
  - global timeout (30 min): auto-abort`,
	}
	c.AddCommand(clusterRotateKeyBegin(), clusterRotateKeyStatus(), clusterRotateKeyAbort())
	c.PersistentFlags().String("data", "./tmp", "GrainFS data directory (must contain rotate.sock)")
	return c
}

func clusterRotateKeyBegin() *cobra.Command {
	c := &cobra.Command{
		Use:   "begin",
		Short: "Start a new rotation with the supplied PSK",
		RunE: func(cmd *cobra.Command, args []string) error {
			dataDir, _ := cmd.Flags().GetString("data")
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
				fmt.Printf("Generated new PSK: %s\n", newKey)
				fmt.Println("Save this securely — you must distribute it to all peers' configs.")
			}
			resp, err := submitRotation(dataDir, rotationSocketRequest{Action: "begin", NewKey: newKey})
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return fmt.Errorf("server: %s", resp.Error)
			}
			fmt.Printf("Rotation started: phase=%d rotation_id=%s\n", resp.Phase, resp.RotationID)
			fmt.Printf("  OLD SPKI: %s\n", resp.OldSPKI)
			fmt.Printf("  NEW SPKI: %s\n", resp.NewSPKI)
			fmt.Println("  Cluster will auto-progress. Watch with 'cluster rotate-key status'.")
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
			dataDir, _ := cmd.Flags().GetString("data")
			resp, err := submitRotation(dataDir, rotationSocketRequest{Action: "status"})
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return fmt.Errorf("server: %s", resp.Error)
			}
			fmt.Printf("phase: %s (%d)\n", phaseLabel(resp.Phase), resp.Phase)
			if resp.RotationID != "" {
				fmt.Printf("rotation_id: %s\n", resp.RotationID)
			}
			if resp.OldSPKI != "" {
				fmt.Printf("old_spki: %s\n", resp.OldSPKI)
			}
			if resp.NewSPKI != "" {
				fmt.Printf("new_spki: %s\n", resp.NewSPKI)
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
			dataDir, _ := cmd.Flags().GetString("data")
			reason, _ := cmd.Flags().GetString("reason")
			resp, err := submitRotation(dataDir, rotationSocketRequest{Action: "abort", Reason: reason})
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return fmt.Errorf("server: %s", resp.Error)
			}
			fmt.Printf("Rotation aborted. Phase now: %s (%d)\n", phaseLabel(resp.Phase), resp.Phase)
			return nil
		},
	}
	c.Flags().String("reason", "operator", "abort reason (logged in raft entry)")
	return c
}

func submitRotation(dataDir string, req rotationSocketRequest) (rotationSocketResponse, error) {
	sock := filepath.Join(dataDir, rotationSocketName)
	conn, err := net.DialTimeout("unix", sock, 5*time.Second)
	if err != nil {
		return rotationSocketResponse{}, fmt.Errorf("dial %s: %w", sock, err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(45 * time.Second))
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return rotationSocketResponse{}, fmt.Errorf("encode request: %w", err)
	}
	var resp rotationSocketResponse
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return rotationSocketResponse{}, fmt.Errorf("decode response: %w", err)
	}
	return resp, nil
}

func phaseLabel(p int) string {
	switch p {
	case 1:
		return "steady"
	case 2:
		return "begun"
	case 3:
		return "switched"
	default:
		return "unknown"
	}
}
