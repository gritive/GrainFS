package main

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/nfsadmin"
)

func nfsExportCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "export",
		Short: "Manage NFSv4 export registrations",
	}
	cmd.AddCommand(
		nfsExportAddCmd(),
		nfsExportRemoveCmd(),
		nfsExportUpdateCmd(),
		nfsExportListCmd(),
	)
	return cmd
}

func nfsExportAddCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "add <bucket>",
		Short: "Register a bucket as an NFSv4 export",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := validateMutationFormatFlags(cmd); err != nil {
				return err
			}
			ro, _ := cmd.Flags().GetBool("ro")
			return nfsadmin.RunAdd(cmd.Context(), nfsadmin.AddExportOptions{
				BaseOptions: nfsBaseOptions(cmd),
				Bucket:      args[0],
				ReadOnly:    ro,
				DryRun:      flagBool(cmd, "dry-run"),
			})
		},
	}
	cmd.Flags().Bool("ro", false, "register as read-only export")
	addStandardExportFlags(cmd)
	return cmd
}

func nfsExportRemoveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "remove <bucket>",
		Aliases: []string{"rm"},
		Short:   "Unregister an NFSv4 export",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := validateMutationFormatFlags(cmd); err != nil {
				return err
			}
			return nfsadmin.RunRemove(cmd.Context(), nfsadmin.RemoveExportOptions{
				BaseOptions: nfsBaseOptions(cmd),
				Bucket:      args[0],
				DryRun:      flagBool(cmd, "dry-run"),
			})
		},
	}
	addStandardExportFlags(cmd)
	return cmd
}

func nfsExportUpdateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update <bucket>",
		Short: "Update an NFSv4 export",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := validateMutationFormatFlags(cmd); err != nil {
				return err
			}
			roChanged := cmd.Flags().Changed("ro")
			rwChanged := cmd.Flags().Changed("rw")
			if !roChanged && !rwChanged {
				return fmt.Errorf("must specify --ro or --rw")
			}
			ro, _ := cmd.Flags().GetBool("ro")
			wait, _ := cmd.Flags().GetDuration("quiesce-wait")
			if wait > 0 && !ro {
				return fmt.Errorf("--quiesce-wait only applies when transitioning to read-only")
			}
			if wait > 0 {
				fmt.Fprintf(cmd.OutOrStdout(), "Waiting %s before updating export %q...\n", wait, args[0])
				time.Sleep(wait)
			}
			return nfsadmin.RunUpdate(cmd.Context(), nfsadmin.UpdateExportOptions{
				BaseOptions: nfsBaseOptions(cmd),
				Bucket:      args[0],
				ReadOnly:    ro,
				DryRun:      flagBool(cmd, "dry-run"),
			})
		},
	}
	cmd.Flags().Bool("ro", false, "set export read-only")
	cmd.Flags().Bool("rw", false, "set export read-write")
	cmd.Flags().Duration("quiesce-wait", 0, "wait before switching to read-only")
	cmd.MarkFlagsMutuallyExclusive("ro", "rw")
	addStandardExportFlags(cmd)
	return cmd
}

func nfsExportListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List NFSv4 exports",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return nfsadmin.RunList(cmd.Context(), nfsadmin.ListExportOptions{BaseOptions: nfsBaseOptions(cmd)})
		},
	}
}

func addStandardExportFlags(cmd *cobra.Command) {
	cmd.Flags().Bool("dry-run", false, "validate only; do not propose change")
	cmd.Flags().Bool("quiet", false, "suppress stdout on success")
}

func nfsBaseOptions(cmd *cobra.Command) nfsadmin.BaseOptions {
	endpoint, err := adminEndpointFromCmd(cmd)
	if err != nil {
		endpoint = ""
	}
	return nfsadmin.BaseOptions{
		Endpoint: endpoint,
		Timeout:  adminTimeoutFromCmd(cmd),
		JSONOut:  jsonOut(cmd),
		Quiet:    flagBool(cmd, "quiet"),
		Stdout:   cmd.OutOrStdout(),
		Stderr:   cmd.ErrOrStderr(),
	}
}

func flagBool(cmd *cobra.Command, name string) bool {
	v, _ := cmd.Flags().GetBool(name)
	return v
}

func validateMutationFormatFlags(cmd *cobra.Command) error {
	if jsonOut(cmd) && flagBool(cmd, "quiet") {
		return fmt.Errorf("--quiet cannot be used with --format json")
	}
	return nil
}
