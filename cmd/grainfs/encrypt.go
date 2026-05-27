package main

import "github.com/spf13/cobra"

var encryptCmd = &cobra.Command{
	Use:   "encrypt",
	Short: "Encryption management commands (admin only)",
}

func init() {
	rootCmd.AddCommand(encryptCmd)
}
