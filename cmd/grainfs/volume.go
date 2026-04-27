package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/spf13/cobra"
)

var volumeCmd = &cobra.Command{
	Use:   "volume",
	Short: "Volume management commands",
}

var volumeRecalculateCmd = &cobra.Command{
	Use:   "recalculate <name>",
	Short: "Recalculate AllocatedBlocks by counting actual block objects",
	Args:  cobra.ExactArgs(1),
	RunE:  runVolumeRecalculate,
}

var volumeCloneCmd = &cobra.Command{
	Use:   "clone <src> <dst>",
	Short: "Clone a volume (fast, block-sharing copy)",
	Args:  cobra.ExactArgs(2),
	RunE:  runVolumeClone,
}

var volumeRollbackCmd = &cobra.Command{
	Use:   "rollback <volume> <snap_id>",
	Short: "Rollback a volume to a snapshot",
	Args:  cobra.ExactArgs(2),
	RunE:  runVolumeRollback,
}

var snapshotCmd = &cobra.Command{
	Use:   "snapshot",
	Short: "Volume snapshot management",
}

var snapshotCreateCmd = &cobra.Command{
	Use:   "create <volume>",
	Short: "Create a snapshot of a volume",
	Args:  cobra.ExactArgs(1),
	RunE:  runSnapshotCreate,
}

var snapshotListCmd = &cobra.Command{
	Use:   "list <volume>",
	Short: "List snapshots for a volume",
	Args:  cobra.ExactArgs(1),
	RunE:  runSnapshotList,
}

var snapshotDeleteCmd = &cobra.Command{
	Use:   "delete <volume> <snap_id>",
	Short: "Delete a snapshot",
	Args:  cobra.ExactArgs(2),
	RunE:  runSnapshotDelete,
}

func addEndpointFlag(cmds ...*cobra.Command) {
	for _, cmd := range cmds {
		cmd.Flags().String("endpoint", "http://localhost:9000", "grainfs server endpoint")
	}
}

func init() {
	addEndpointFlag(volumeRecalculateCmd, volumeCloneCmd, volumeRollbackCmd,
		snapshotCreateCmd, snapshotListCmd, snapshotDeleteCmd)
	snapshotCmd.AddCommand(snapshotCreateCmd, snapshotListCmd, snapshotDeleteCmd)
	volumeCmd.AddCommand(volumeRecalculateCmd, volumeCloneCmd, volumeRollbackCmd, snapshotCmd)
	rootCmd.AddCommand(volumeCmd)
}

func volumeEndpoint(cmd *cobra.Command) string {
	ep, _ := cmd.Flags().GetString("endpoint")
	return ep
}

func runVolumeRecalculate(cmd *cobra.Command, args []string) error {
	name := args[0]
	endpoint := volumeEndpoint(cmd)

	url := fmt.Sprintf("%s/volumes/%s/recalculate", endpoint, name)
	resp, err := http.Post(url, "application/json", nil) //nolint:noctx
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned %d: %s", resp.StatusCode, body)
	}

	var result struct {
		Volume string `json:"volume"`
		Before int64  `json:"before"`
		After  int64  `json:"after"`
		Fixed  bool   `json:"fixed"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}

	status := "no change"
	if result.Fixed {
		status = "fixed"
	}
	fmt.Printf("recalculated %q: %d → %d (%s)\n", result.Volume, result.Before, result.After, status)
	return nil
}

func runVolumeClone(cmd *cobra.Command, args []string) error {
	src, dst := args[0], args[1]
	endpoint := volumeEndpoint(cmd)

	body, _ := json.Marshal(map[string]string{"src": src, "dst": dst})
	resp, err := http.Post(endpoint+"/volumes/clone", "application/json", bytes.NewReader(body)) //nolint:noctx
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("server returned %d: %s", resp.StatusCode, respBody)
	}
	fmt.Printf("cloned %q → %q\n", src, dst)
	return nil
}

func runVolumeRollback(cmd *cobra.Command, args []string) error {
	name, snapID := args[0], args[1]
	endpoint := volumeEndpoint(cmd)

	url := fmt.Sprintf("%s/volumes/%s/snapshots/%s/rollback", endpoint, name, snapID)
	resp, err := http.Post(url, "application/json", nil) //nolint:noctx
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned %d: %s", resp.StatusCode, body)
	}
	fmt.Printf("rolled back %q to snapshot %q\n", name, snapID)
	return nil
}

func runSnapshotCreate(cmd *cobra.Command, args []string) error {
	name := args[0]
	endpoint := volumeEndpoint(cmd)

	url := fmt.Sprintf("%s/volumes/%s/snapshots", endpoint, name)
	resp, err := http.Post(url, "application/json", nil) //nolint:noctx
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("server returned %d: %s", resp.StatusCode, body)
	}

	var result struct {
		ID         string `json:"id"`
		BlockCount int64  `json:"block_count"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}
	fmt.Printf("snapshot %q created (blocks: %d)\n", result.ID, result.BlockCount)
	return nil
}

func runSnapshotList(cmd *cobra.Command, args []string) error {
	name := args[0]
	endpoint := volumeEndpoint(cmd)

	url := fmt.Sprintf("%s/volumes/%s/snapshots", endpoint, name)
	resp, err := http.Get(url) //nolint:noctx
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned %d: %s", resp.StatusCode, body)
	}

	var snaps []struct {
		ID         string `json:"id"`
		CreatedAt  string `json:"created_at"`
		BlockCount int64  `json:"block_count"`
	}
	if err := json.Unmarshal(body, &snaps); err != nil {
		return fmt.Errorf("parse response: %w", err)
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
	name, snapID := args[0], args[1]
	endpoint := volumeEndpoint(cmd)

	url := fmt.Sprintf("%s/volumes/%s/snapshots/%s", endpoint, name, snapID)
	req, _ := http.NewRequest(http.MethodDelete, url, nil)
	resp, err := http.DefaultClient.Do(req) //nolint:noctx
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned %d: %s", resp.StatusCode, body)
	}
	fmt.Printf("snapshot %q deleted from %q\n", snapID, name)
	return nil
}
