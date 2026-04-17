package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/spf13/cobra"
)

var clusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Cluster management commands",
}

var clusterStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show cluster status (leader, term, peers)",
	RunE: func(cmd *cobra.Command, args []string) error {
		endpoint, _ := cmd.Flags().GetString("endpoint")
		format, _ := cmd.Flags().GetString("format")

		resp, err := http.Get(endpoint + "/api/cluster/status")
		if err != nil {
			return fmt.Errorf("connect to %s: %w", endpoint, err)
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("read response: %w", err)
		}

		if format == "text" {
			var status map[string]any
			if err := json.Unmarshal(body, &status); err != nil {
				return fmt.Errorf("parse response: %w", err)
			}
			printClusterStatus(status)
			return nil
		}

		// JSON output (default for scripting)
		fmt.Println(string(body))
		return nil
	},
}

func printClusterStatus(s map[string]any) {
	mode, _ := s["mode"].(string)
	fmt.Printf("mode:      %s\n", mode)
	if mode == "cluster" {
		fmt.Printf("node_id:   %v\n", s["node_id"])
		fmt.Printf("state:     %v\n", s["state"])
		fmt.Printf("term:      %v\n", s["term"])
		fmt.Printf("leader_id: %v\n", s["leader_id"])
		if peers, ok := s["peers"].([]any); ok {
			fmt.Printf("peers:     %d\n", len(peers))
			for _, p := range peers {
				fmt.Printf("  - %v\n", p)
			}
		}
	}
}

func init() {
	clusterStatusCmd.Flags().String("endpoint", "http://127.0.0.1:9000", "GrainFS server endpoint")
	clusterStatusCmd.Flags().String("format", "json", "Output format: json or text")
	clusterCmd.AddCommand(clusterStatusCmd)
	rootCmd.AddCommand(clusterCmd)
}
