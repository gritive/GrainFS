package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/volumeadmin"
)

// scrubBucketReq mirrors admin.ScrubReq for CLI → admin POST /v1/scrub.
type scrubBucketReq struct {
	Bucket    string `json:"bucket"`
	KeyPrefix string `json:"key_prefix,omitempty"`
	Scope     string `json:"scope,omitempty"`
	DryRun    bool   `json:"dry_run,omitempty"`
}

func newScrubCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "scrub <bucket>",
		Short: "Trigger a cluster-wide EC scrub on the given bucket",
		Long: "Trigger a cluster-wide EC scrub on the given bucket. The trigger\n" +
			"replicates via meta-raft so every node's Director runs the scrub on its\n" +
			"locally-owned groups; cluster-wide stat aggregation is reflected in the\n" +
			"per-session GET endpoint. Without --detach the command follows the\n" +
			"session until it reaches done|cancelled.",
		Args: cobra.ExactArgs(1),
		RunE: runBucketScrub,
	}
	cmd.Flags().String("prefix", "", "narrow walk to keys starting with this prefix")
	cmd.Flags().String("scope", "full", "scope: full | live")
	cmd.Flags().Bool("dry-run", false, "observe only, no repair")
	cmd.Flags().Bool("detach", false, "don't follow, return immediately")
	registerAdminTimeoutFlag(cmd)
	return cmd
}

func runBucketScrub(cmd *cobra.Command, args []string) error {
	c, err := adminClientFromCmd(cmd)
	if err != nil {
		return err
	}
	bucket := args[0]
	prefix, _ := cmd.Flags().GetString("prefix")
	scope, _ := cmd.Flags().GetString("scope")
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	detach, _ := cmd.Flags().GetBool("detach")

	ctx, cancel := applyAdminTimeout(cmd.Context(), cmd)
	defer cancel()

	body := scrubBucketReq{Bucket: bucket, KeyPrefix: prefix, Scope: scope, DryRun: dryRun}
	var resp volumeadmin.ScrubTriggerResp
	if err := c.Post(ctx, "/v1/scrub", body, &resp); err != nil {
		return err
	}
	if jsonOut(cmd) {
		return printJSON(cmd, resp)
	}
	created := "reused"
	if resp.Created {
		created = "created"
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Triggered scrub: bucket=%s session=%s scope=%s dry_run=%t (%s)\n",
		bucket, resp.SessionID, scope, dryRun, created)
	if detach {
		return nil
	}
	return volumeadmin.FollowScrubSession(ctx, c, cmd.OutOrStdout(), resp.SessionID, 0)
}

func init() {
	rootCmd.AddCommand(newScrubCmd())
}
