package main

import (
	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/iamadmin"
)

var iamExplainCmd = &cobra.Command{
	Use:   "explain --sa <sa_id> --s3 <verb> <s3://bucket[/key]>",
	Short: "Explain an IAM decision for a request-shaped S3 operation",
	Example: `  grainfs iam explain --sa sa-abc123 --s3 put s3://logs/2026/entry.json
  grainfs iam --json explain --sa sa-abc123 --s3 ls s3://logs`,
	Args: cobra.ExactArgs(1),
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		said, _ := c.Flags().GetString("sa")
		s3Verb, _ := c.Flags().GetString("s3")
		return iamadmin.RunExplain(c.Context(), iamadmin.ExplainOptions{
			BaseOptions: base,
			SAID:        said,
			S3Verb:      s3Verb,
			S3URI:       args[0],
		})
	},
}

func init() {
	iamExplainCmd.Flags().String("sa", "", "ServiceAccount ID")
	iamExplainCmd.Flags().String("s3", "", "S3 verb to explain (get, put, delete, list)")
	_ = iamExplainCmd.MarkFlagRequired("sa")
	_ = iamExplainCmd.MarkFlagRequired("s3")
	iamCmd.AddCommand(iamExplainCmd)
}
