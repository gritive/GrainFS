package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

// --- Cobra command tree ---

var volumeCmd = &cobra.Command{
	Use:   "volume",
	Short: "Volume management commands",
}

var volumeListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all volumes",
	Example: `  # 기본 표 형식
  grainfs volume list

  # JSON 출력 (스크립팅용)
  grainfs volume list --json

  # 원시 바이트 단위 (raw int)
  grainfs volume list --bytes`,
	RunE: runVolumeList,
}

var volumeCreateCmd = &cobra.Command{
	Use:   "create <name>",
	Short: "Create a new volume",
	Args:  cobra.ExactArgs(1),
	Example: `  # 1 GiB 볼륨 생성
  grainfs volume create v1 --size 1G

  # 절대 바이트 지정
  grainfs volume create v2 --size 1073741824`,
	RunE: runVolumeCreate,
}

var volumeInfoCmd = &cobra.Command{
	Use:     "info <name>",
	Short:   "Show volume metadata",
	Args:    cobra.ExactArgs(1),
	Example: `  grainfs volume info v1`,
	RunE:    runVolumeInfo,
}

var volumeStatCmd = &cobra.Command{
	Use:     "stat <name>",
	Short:   "Show volume usage and recent incidents",
	Args:    cobra.ExactArgs(1),
	Example: `  grainfs volume stat v1`,
	RunE:    runVolumeStat,
}

var volumeDeleteCmd = &cobra.Command{
	Use:   "delete <name>",
	Short: "Delete a volume (refuses if snapshots exist; --force cascades)",
	Args:  cobra.ExactArgs(1),
	Example: `  # snapshot 없는 볼륨 삭제
  grainfs volume delete v1

  # snapshot까지 일괄 삭제
  grainfs volume delete v1 --force`,
	RunE: runVolumeDelete,
}

var volumeResizeCmd = &cobra.Command{
	Use:   "resize <name>",
	Short: "Resize a volume (grow only — shrink is rejected)",
	Args:  cobra.ExactArgs(1),
	Example: `  # 2 GiB로 늘림
  grainfs volume resize v1 --size 2G`,
	RunE: runVolumeResize,
}

var volumeRecalculateCmd = &cobra.Command{
	Use:     "recalculate <name>",
	Short:   "Recalculate AllocatedBlocks by counting actual block objects",
	Args:    cobra.ExactArgs(1),
	Example: `  grainfs volume recalculate v1`,
	RunE:    runVolumeRecalculate,
}

var volumeCloneCmd = &cobra.Command{
	Use:     "clone <src> <dst>",
	Short:   "Clone a volume (fast, block-sharing copy)",
	Args:    cobra.ExactArgs(2),
	Example: `  grainfs volume clone v1 v1-copy`,
	RunE:    runVolumeClone,
}

var volumeRollbackCmd = &cobra.Command{
	Use:     "rollback <volume> <snap_id>",
	Short:   "Rollback a volume to a snapshot",
	Args:    cobra.ExactArgs(2),
	Example: `  grainfs volume rollback v1 0192e8a4-...`,
	RunE:    runVolumeRollback,
}

var snapshotCmd = &cobra.Command{
	Use:   "snapshot",
	Short: "Volume snapshot management",
}

var snapshotCreateCmd = &cobra.Command{
	Use:     "create <volume>",
	Short:   "Create a snapshot of a volume",
	Args:    cobra.ExactArgs(1),
	Example: `  grainfs volume snapshot create v1`,
	RunE:    runSnapshotCreate,
}

var snapshotListCmd = &cobra.Command{
	Use:     "list <volume>",
	Short:   "List snapshots for a volume",
	Args:    cobra.ExactArgs(1),
	Example: `  grainfs volume snapshot list v1`,
	RunE:    runSnapshotList,
}

var snapshotDeleteCmd = &cobra.Command{
	Use:     "delete <volume> <snap_id>",
	Short:   "Delete a snapshot",
	Args:    cobra.ExactArgs(2),
	Example: `  grainfs volume snapshot delete v1 0192e8a4-...`,
	RunE:    runSnapshotDelete,
}

func init() {
	for _, c := range []*cobra.Command{
		volumeListCmd, volumeCreateCmd, volumeInfoCmd, volumeStatCmd,
		volumeDeleteCmd, volumeResizeCmd, volumeRecalculateCmd, volumeCloneCmd,
		volumeRollbackCmd, snapshotCreateCmd, snapshotListCmd, snapshotDeleteCmd,
	} {
		c.Flags().String("endpoint", "", "admin endpoint (default: auto-discover from --data or grainfs.toml)")
		c.Flags().String("data", "", "data directory for admin socket auto-discovery")
		c.Flags().Bool("json", false, "JSON output for scripting")
		c.Flags().Bool("bytes", false, "show sizes as raw byte counts (alias: --raw)")
		c.Flags().Bool("raw", false, "alias for --bytes")
	}
	volumeCreateCmd.Flags().String("size", "", "volume size (1G/1Gi/100M/raw bytes)")
	volumeResizeCmd.Flags().String("size", "", "new size (must be >= current)")
	volumeDeleteCmd.Flags().Bool("force", false, "cascade-delete all snapshots")

	snapshotCmd.AddCommand(snapshotCreateCmd, snapshotListCmd, snapshotDeleteCmd)
	volumeCmd.AddCommand(
		volumeListCmd, volumeCreateCmd, volumeInfoCmd, volumeStatCmd,
		volumeDeleteCmd, volumeResizeCmd, volumeRecalculateCmd,
		volumeCloneCmd, volumeRollbackCmd, snapshotCmd,
	)
	rootCmd.AddCommand(volumeCmd)
}

// --- Helpers ---

func adminClientFromCmd(cmd *cobra.Command) (*adminClient, error) {
	endpoint, _ := cmd.Flags().GetString("endpoint")
	dataFlag, _ := cmd.Flags().GetString("data")
	return newAdminClient(endpoint, dataFlag)
}

func jsonOut(cmd *cobra.Command) bool {
	v, _ := cmd.Flags().GetBool("json")
	return v
}

func rawBytes(cmd *cobra.Command) bool {
	a, _ := cmd.Flags().GetBool("bytes")
	b, _ := cmd.Flags().GetBool("raw")
	return a || b
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// --- Runners ---

type volumeJSON struct {
	Name            string `json:"name"`
	Size            int64  `json:"size"`
	BlockSize       int    `json:"block_size"`
	AllocatedBlocks int64  `json:"allocated_blocks"`
	AllocatedBytes  int64  `json:"allocated_bytes"`
	SnapshotCount   int32  `json:"snapshot_count"`
}

func runVolumeList(cmd *cobra.Command, args []string) error {
	c, err := adminClientFromCmd(cmd)
	if err != nil {
		return err
	}
	var resp struct {
		Volumes []volumeJSON `json:"volumes"`
	}
	if err := c.get("/v1/volumes", &resp); err != nil {
		return err
	}
	if jsonOut(cmd) {
		return printJSON(resp)
	}
	raw := rawBytes(cmd)
	fmt.Printf("%-20s  %12s  %12s  %8s  %s\n", "NAME", "SIZE", "ALLOCATED", "BLOCK", "SNAPS")
	if len(resp.Volumes) == 0 {
		fmt.Println("(no volumes)")
		return nil
	}
	for _, v := range resp.Volumes {
		fmt.Printf("%-20s  %12s  %12s  %8s  %d\n",
			v.Name, formatBytes(v.Size, raw), formatBytes(v.AllocatedBytes, raw),
			formatBytes(int64(v.BlockSize), raw), v.SnapshotCount)
	}
	return nil
}

func runVolumeCreate(cmd *cobra.Command, args []string) error {
	sizeStr, _ := cmd.Flags().GetString("size")
	if sizeStr == "" {
		return fmt.Errorf("--size required")
	}
	sz, err := parseSize(sizeStr)
	if err != nil {
		return fmt.Errorf("invalid --size: %w", err)
	}
	c, err := adminClientFromCmd(cmd)
	if err != nil {
		return err
	}
	body := map[string]any{"name": args[0], "size": sz}
	var resp volumeJSON
	if err := c.post("/v1/volumes", body, &resp); err != nil {
		return err
	}
	if jsonOut(cmd) {
		return printJSON(resp)
	}
	fmt.Printf("created %q (size=%s, block=%d)\n", resp.Name, formatBytes(resp.Size, false), resp.BlockSize)
	return nil
}

func runVolumeInfo(cmd *cobra.Command, args []string) error {
	c, err := adminClientFromCmd(cmd)
	if err != nil {
		return err
	}
	var resp volumeJSON
	if err := c.get("/v1/volumes/"+url.PathEscape(args[0]), &resp); err != nil {
		return err
	}
	if jsonOut(cmd) {
		return printJSON(resp)
	}
	raw := rawBytes(cmd)
	fmt.Printf("name:             %s\n", resp.Name)
	fmt.Printf("size:             %s\n", formatBytes(resp.Size, raw))
	fmt.Printf("block_size:       %s\n", formatBytes(int64(resp.BlockSize), raw))
	fmt.Printf("allocated_bytes:  %s\n", formatBytes(resp.AllocatedBytes, raw))
	fmt.Printf("allocated_blocks: %d\n", resp.AllocatedBlocks)
	fmt.Printf("snapshot_count:   %d\n", resp.SnapshotCount)
	return nil
}

func runVolumeStat(cmd *cobra.Command, args []string) error {
	c, err := adminClientFromCmd(cmd)
	if err != nil {
		return err
	}
	var resp struct {
		Volume          volumeJSON       `json:"volume"`
		RecentIncidents []map[string]any `json:"recent_incidents"`
	}
	if err := c.get("/v1/volumes/"+url.PathEscape(args[0])+"/stat", &resp); err != nil {
		return err
	}
	if jsonOut(cmd) {
		return printJSON(resp)
	}
	raw := rawBytes(cmd)
	fmt.Printf("volume:           %s\n", resp.Volume.Name)
	fmt.Printf("size:             %s\n", formatBytes(resp.Volume.Size, raw))
	fmt.Printf("allocated:        %s\n", formatBytes(resp.Volume.AllocatedBytes, raw))
	fmt.Printf("snapshots:        %d\n", resp.Volume.SnapshotCount)
	if len(resp.RecentIncidents) > 0 {
		fmt.Printf("recent incidents: %d\n", len(resp.RecentIncidents))
	}
	return nil
}

func runVolumeDelete(cmd *cobra.Command, args []string) error {
	force, _ := cmd.Flags().GetBool("force")
	c, err := adminClientFromCmd(cmd)
	if err != nil {
		return err
	}
	path := "/v1/volumes/" + url.PathEscape(args[0])
	if force {
		path += "?force=true"
	}
	var resp struct {
		Deleted bool `json:"deleted"`
	}
	err = c.delete(path, &resp)
	if cerr, ok := err.(*cliError); ok && cerr.Code == "conflict" && !jsonOut(cmd) {
		printDeleteConflict(cerr)
		return cerr
	}
	if err != nil {
		return err
	}
	if jsonOut(cmd) {
		return printJSON(resp)
	}
	fmt.Printf("deleted %q\n", args[0])
	return nil
}

func printDeleteConflict(cerr *cliError) {
	fmt.Fprintf(os.Stderr, "%s\n", cerr.Message)
	if cerr.Details == nil {
		return
	}
	if recent, ok := cerr.Details["recent"].([]any); ok && len(recent) > 0 {
		fmt.Fprintln(os.Stderr, "  Recent snapshots:")
		for _, r := range recent {
			if s, ok := r.(map[string]any); ok {
				fmt.Fprintf(os.Stderr, "    %v  %v  blocks=%v\n", s["id"], s["created_at"], s["block_count"])
			}
		}
	}
	if cmdStr, ok := cerr.Details["cascade_command"].(string); ok {
		fmt.Fprintf(os.Stderr, "  Cascade:  %s\n", cmdStr)
	}
	if cmdStr, ok := cerr.Details["list_command"].(string); ok {
		fmt.Fprintf(os.Stderr, "  Or list:  %s\n", cmdStr)
	}
}

func runVolumeResize(cmd *cobra.Command, args []string) error {
	sizeStr, _ := cmd.Flags().GetString("size")
	if sizeStr == "" {
		return fmt.Errorf("--size required")
	}
	sz, err := parseSize(sizeStr)
	if err != nil {
		return fmt.Errorf("invalid --size: %w", err)
	}
	c, err := adminClientFromCmd(cmd)
	if err != nil {
		return err
	}
	var resp struct {
		Name    string `json:"name"`
		OldSize int64  `json:"old_size"`
		NewSize int64  `json:"new_size"`
		Changed bool   `json:"changed"`
	}
	err = c.post("/v1/volumes/"+url.PathEscape(args[0])+"/resize", map[string]any{"size": sz}, &resp)
	if cerr, ok := err.(*cliError); ok && cerr.Code == "unsupported" && !jsonOut(cmd) {
		fmt.Fprintln(os.Stderr, cerr.Message)
		if cerr.Details != nil {
			if hint, ok := cerr.Details["hint"].(string); ok {
				fmt.Fprintf(os.Stderr, "  Hint:  %s\n", hint)
			}
			if cmdStr, ok := cerr.Details["clone_command"].(string); ok {
				fmt.Fprintf(os.Stderr, "  Try:   %s\n", cmdStr)
			}
		}
		return cerr
	}
	if err != nil {
		return err
	}
	if jsonOut(cmd) {
		return printJSON(resp)
	}
	if !resp.Changed {
		fmt.Printf("no change (size already %s)\n", formatBytes(resp.NewSize, false))
		return nil
	}
	fmt.Printf("resized %q: %s → %s\n", resp.Name,
		formatBytes(resp.OldSize, false), formatBytes(resp.NewSize, false))
	return nil
}

func runVolumeRecalculate(cmd *cobra.Command, args []string) error {
	c, err := adminClientFromCmd(cmd)
	if err != nil {
		return err
	}
	var resp struct {
		Volume string `json:"volume"`
		Before int64  `json:"before"`
		After  int64  `json:"after"`
		Fixed  bool   `json:"fixed"`
	}
	if err := c.post("/v1/volumes/"+url.PathEscape(args[0])+"/recalculate", nil, &resp); err != nil {
		return err
	}
	if jsonOut(cmd) {
		return printJSON(resp)
	}
	status := "no change"
	if resp.Fixed {
		status = "fixed"
	}
	fmt.Printf("recalculated %q: %d → %d (%s)\n", resp.Volume, resp.Before, resp.After, status)
	return nil
}

func runVolumeClone(cmd *cobra.Command, args []string) error {
	c, err := adminClientFromCmd(cmd)
	if err != nil {
		return err
	}
	if err := c.post("/v1/volumes/clone", map[string]any{"src": args[0], "dst": args[1]}, nil); err != nil {
		return err
	}
	if !jsonOut(cmd) {
		fmt.Printf("cloned %q → %q\n", args[0], args[1])
	}
	return nil
}

func runVolumeRollback(cmd *cobra.Command, args []string) error {
	c, err := adminClientFromCmd(cmd)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("/v1/volumes/%s/snapshots/%s/rollback", url.PathEscape(args[0]), url.PathEscape(args[1]))
	if err := c.post(path, nil, nil); err != nil {
		return err
	}
	if !jsonOut(cmd) {
		fmt.Printf("rolled back %q to snapshot %q\n", args[0], args[1])
	}
	return nil
}

func runSnapshotCreate(cmd *cobra.Command, args []string) error {
	c, err := adminClientFromCmd(cmd)
	if err != nil {
		return err
	}
	var resp struct {
		ID         string `json:"id"`
		BlockCount int64  `json:"block_count"`
	}
	if err := c.post("/v1/volumes/"+url.PathEscape(args[0])+"/snapshots", nil, &resp); err != nil {
		return err
	}
	if jsonOut(cmd) {
		return printJSON(resp)
	}
	fmt.Printf("snapshot %q created (blocks: %d)\n", resp.ID, resp.BlockCount)
	return nil
}

func runSnapshotList(cmd *cobra.Command, args []string) error {
	c, err := adminClientFromCmd(cmd)
	if err != nil {
		return err
	}
	var snaps []struct {
		ID         string `json:"id"`
		CreatedAt  string `json:"created_at"`
		BlockCount int64  `json:"block_count"`
	}
	if err := c.get("/v1/volumes/"+url.PathEscape(args[0])+"/snapshots", &snaps); err != nil {
		return err
	}
	if jsonOut(cmd) {
		return printJSON(snaps)
	}
	if len(snaps) == 0 {
		fmt.Println("no snapshots")
		return nil
	}
	fmt.Printf("%-40s  %-30s  %s\n", "ID", "CREATED AT", "BLOCKS")
	fmt.Println(strings.Repeat("-", 80))
	for _, s := range snaps {
		fmt.Printf("%-40s  %-30s  %d\n", s.ID, s.CreatedAt, s.BlockCount)
	}
	return nil
}

func runSnapshotDelete(cmd *cobra.Command, args []string) error {
	c, err := adminClientFromCmd(cmd)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("/v1/volumes/%s/snapshots/%s", url.PathEscape(args[0]), url.PathEscape(args[1]))
	if err := c.delete(path, nil); err != nil {
		return err
	}
	if !jsonOut(cmd) {
		fmt.Printf("snapshot %q deleted from %q\n", args[1], args[0])
	}
	return nil
}
