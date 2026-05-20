package main

import (
	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/nfsadmin"
)

func nfsDebugCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "debug <bucket>",
		Short: "Diagnose an NFS bucket export",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return nfsadmin.RunDebug(cmd.Context(), nfsadmin.DebugExportOptions{
				BaseOptions: nfsBaseOptions(cmd),
				Bucket:      args[0],
			})
		},
	}
	cmd.Flags().Bool("json", false, "emit machine-readable JSON output")
	return cmd
}
