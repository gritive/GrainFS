package main

import (
	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/icebergadmin"
)

var icebergCmd = &cobra.Command{
	Use:   "iceberg",
	Short: "Iceberg REST Catalog helpers",
}

var icebergConfigCmd = &cobra.Command{
	Use:   "config",
	Short: "Print OAuth2 connection bundle for Iceberg clients",
	Example: `  grainfs iceberg config --warehouse analytics --sa sa-abc123
  grainfs iceberg config --warehouse analytics --sa sa-abc123 --no-reveal
  grainfs iceberg config --warehouse analytics --sa sa-abc123 --json`,
	RunE: func(c *cobra.Command, _ []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		warehouse, _ := c.Flags().GetString("warehouse")
		said, _ := c.Flags().GetString("sa")
		noReveal, _ := c.Flags().GetBool("no-reveal")
		return icebergadmin.RunIcebergConfig(c.Context(), icebergadmin.IcebergConfigOptions{
			BaseOptions: base,
			Warehouse:   warehouse,
			SAID:        said,
			NoReveal:    noReveal,
		})
	},
}

func init() {
	icebergCmd.PersistentFlags().String("endpoint", "",
		"admin Unix socket path (overrides GRAINFS_ADMIN_SOCKET env var)")
	icebergCmd.PersistentFlags().Bool("json", false, "output raw JSON")

	icebergConfigCmd.Flags().String("warehouse", "", "Iceberg warehouse name (required)")
	icebergConfigCmd.Flags().String("sa", "", "ServiceAccount ID whose key to reveal (required)")
	icebergConfigCmd.Flags().Bool("no-reveal", false, "hide client_secret from output (server never sends it)")

	icebergCmd.AddCommand(icebergConfigCmd)
	rootCmd.AddCommand(icebergCmd)
}
