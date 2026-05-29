package main

import (
	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/iamadmin"
)

var iamPDPCmd = &cobra.Command{
	Use:   "pdp",
	Short: "Manage the External PDP bearer token (sealed at rest)",
}

var iamPDPSetTokenCmd = &cobra.Command{
	Use:   "set-token --token-file <path>",
	Short: "Set the External PDP bearer token from a file (sealed before storage)",
	Example: `  grainfs iam pdp set-token --token-file /run/secrets/pdp-token
  grainfs iam --endpoint ./tmp/admin.sock pdp set-token --token-file ./token`,
	RunE: func(c *cobra.Command, _ []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		tokenFile, _ := c.Flags().GetString("token-file")
		return iamadmin.RunPDPSetToken(c.Context(), iamadmin.PDPSetTokenOptions{
			BaseOptions: base, TokenFile: tokenFile,
		})
	},
}

var iamPDPClearTokenCmd = &cobra.Command{
	Use:     "clear-token",
	Short:   "Clear the configured External PDP bearer token",
	Example: `  grainfs iam pdp clear-token`,
	RunE: func(c *cobra.Command, _ []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return iamadmin.RunPDPClearToken(c.Context(), iamadmin.PDPClearTokenOptions{BaseOptions: base})
	},
}

var iamPDPShowCmd = &cobra.Command{
	Use:     "show",
	Short:   "Show External PDP status (endpoint, TLS, token fingerprint; never the token)",
	Example: `  grainfs iam pdp show`,
	RunE: func(c *cobra.Command, _ []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return iamadmin.RunPDPShow(c.Context(), iamadmin.PDPShowOptions{BaseOptions: base})
	},
}

func init() {
	iamPDPSetTokenCmd.Flags().String("token-file", "",
		"path to a file containing the bearer token (required; trailing newline trimmed)")
	_ = iamPDPSetTokenCmd.MarkFlagRequired("token-file")

	iamPDPCmd.AddCommand(iamPDPSetTokenCmd, iamPDPClearTokenCmd, iamPDPShowCmd)
	iamCmd.AddCommand(iamPDPCmd)
}
