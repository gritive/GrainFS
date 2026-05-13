package main

import "github.com/spf13/cobra"

var nfsCmd = &cobra.Command{
	Use:   "nfs",
	Short: "Manage NFSv4 exports",
	Long: `NFSv4 export administration.

GrainFS exports each S3 bucket as an independent NFSv4 mount point.
Use 'grainfs nfs export add <bucket>' to register a bucket as an NFS export.`,
}

func init() {
	registerAdminEndpointFlag(nfsCmd)
	registerAdminTimeoutFlag(nfsCmd)
	nfsCmd.PersistentFlags().String("format", "text", "output format: text|json")
	nfsCmd.AddCommand(nfsExportCmd())
	rootCmd.AddCommand(nfsCmd)
}
