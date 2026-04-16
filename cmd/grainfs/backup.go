package main

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
)

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Backup GrainFS data using restic",
	Long: `Backup creates a restic backup of the GrainFS data directory.

Prerequisites:
  1. Install restic: brew install restic (or apt-get install restic)
  2. Initialize restic repo: restic init --repo /path/to/backup

Examples:
  grainfs backup --repo /backup/path --data ./grainfs/data
  grainfs backup --repo s3:s3.amazonaws.com/bucket --data ./grainfs/data`,
	RunE: func(cmd *cobra.Command, args []string) error {
		repo, _ := cmd.Flags().GetString("repo")
		dataDir, _ := cmd.Flags().GetString("data")
		tag, _ := cmd.Flags().GetString("tag")

		if repo == "" {
			return fmt.Errorf("--repo is required (restic repository path)")
		}

		// Verify restic is installed
		if _, err := exec.LookPath("restic"); err != nil {
			return fmt.Errorf("restic not found. Install with: brew install restic")
		}

		// Verify data directory exists
		if _, err := os.Stat(dataDir); os.IsNotExist(err) {
			return fmt.Errorf("data directory not found: %s", dataDir)
		}

		fmt.Printf("🔙 Backing up GrainFS data...\n")
		fmt.Printf("Repository: %s\n", repo)
		fmt.Printf("Data directory: %s\n", dataDir)
		fmt.Printf("Tag: %s\n\n", tag)

		// Build restic command
		resticArgs := []string{"backup", dataDir, "--repo", repo}
		if tag != "" {
			resticArgs = append(resticArgs, "--tag", tag)
		}

		resticCmd := exec.Command("restic", resticArgs...)
		resticCmd.Stdout = os.Stdout
		resticCmd.Stderr = os.Stderr

		if err := resticCmd.Run(); err != nil {
			return fmt.Errorf("backup failed: %w", err)
		}

		fmt.Println("\n✅ Backup completed successfully")
		fmt.Println("\nTo list backups:")
		fmt.Printf("  restic snapshots --repo %s\n", repo)
		fmt.Println("\nTo restore:")
		fmt.Printf("  restic restore latest --repo %s --target /restore/path\n", repo)

		return nil
	},
}

var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore GrainFS data from restic backup",
	Long: `Restore extracts a restic backup to the specified directory.

Examples:
  grainfs restore --repo /backup/path --target ./restore-data
  grainfs restore --repo /backup/path --snapshot <snapshot-id> --target ./restore`,
	RunE: func(cmd *cobra.Command, args []string) error {
		repo, _ := cmd.Flags().GetString("repo")
		target, _ := cmd.Flags().GetString("target")
		snapshot, _ := cmd.Flags().GetString("snapshot")

		if repo == "" {
			return fmt.Errorf("--repo is required")
		}
		if target == "" {
			return fmt.Errorf("--target is required")
		}

		// Verify restic is installed
		if _, err := exec.LookPath("restic"); err != nil {
			return fmt.Errorf("restic not found")
		}

		fmt.Printf("🔓 Restoring GrainFS data...\n")
		fmt.Printf("Repository: %s\n", repo)
		fmt.Printf("Target: %s\n", target)
		if snapshot != "" {
			fmt.Printf("Snapshot: %s\n", snapshot)
		}
		fmt.Println()

		// Build restic restore command
		snapshotID := "latest"
		if snapshot != "" {
			snapshotID = snapshot
		}

		resticArgs := []string{"restore", snapshotID, "--repo", repo, "--target", target}

		resticCmd := exec.Command("restic", resticArgs...)
		resticCmd.Stdout = os.Stdout
		resticCmd.Stderr = os.Stderr

		if err := resticCmd.Run(); err != nil {
			return fmt.Errorf("restore failed: %w", err)
		}

		fmt.Println("\n✅ Restore completed successfully")
		return nil
	},
}

func init() {
	backupCmd.Flags().String("repo", "", "Restic repository path (required)")
	backupCmd.Flags().String("data", "./data", "GrainFS data directory")
	backupCmd.Flags().String("tag", "grainfs", "Backup tag")
	rootCmd.AddCommand(backupCmd)

	restoreCmd.Flags().String("repo", "", "Restic repository path (required)")
	restoreCmd.Flags().String("target", "", "Restore target directory (required)")
	restoreCmd.Flags().String("snapshot", "", "Snapshot ID (default: latest)")
	rootCmd.AddCommand(restoreCmd)
}
