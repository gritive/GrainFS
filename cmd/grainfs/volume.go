package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

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

func init() {
	volumeRecalculateCmd.Flags().String("endpoint", "http://localhost:9000", "grainfs server endpoint")
	volumeCmd.AddCommand(volumeRecalculateCmd)
	rootCmd.AddCommand(volumeCmd)
}

func runVolumeRecalculate(cmd *cobra.Command, args []string) error {
	name := args[0]
	endpoint, _ := cmd.Flags().GetString("endpoint")

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
