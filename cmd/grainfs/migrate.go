package main

import (
	"log/slog"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/cluster"
)

func init() {
	migrateCmd.Flags().StringP("data", "d", "./data", "data directory")
	migrateCmd.Flags().String("node-id", "", "node ID for the seed node (auto-generated if omitted)")
	rootCmd.AddCommand(migrateCmd)
}

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Migrate solo data to cluster format (re-propose metadata through Raft)",
	RunE:  runMigrate,
}

func runMigrate(cmd *cobra.Command, args []string) error {
	dataDir, _ := cmd.Flags().GetString("data")
	nodeID, _ := cmd.Flags().GetString("node-id")

	if nodeID == "" {
		nodeID = generateNodeID(dataDir)
		slog.Info("auto-generated node ID", "component", "migrate", "node_id", nodeID)
	}

	return cluster.MigrateSoloToCluster(dataDir, nodeID)
}
