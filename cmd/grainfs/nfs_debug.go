package main

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/nfsadmin"
)

func nfsDebugCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "debug <bucket>",
		Short: "Diagnose an NFS bucket export",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := nfsadmin.NewClient(nfsBaseOptions(cmd).Endpoint)
			if err != nil {
				return err
			}
			resp, err := client.ExportDebug(cmd.Context(), args[0])
			if err != nil {
				if !nfsJSONOut(cmd) {
					nfsadmin.RenderError(cmd.ErrOrStderr(), err)
				}
				return err
			}
			if nfsJSONOut(cmd) {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(resp)
			}
			return renderExportDebug(cmd.OutOrStdout(), resp)
		},
	}
	cmd.Flags().Bool("json", false, "emit machine-readable JSON output")
	return cmd
}

func renderExportDebug(w io.Writer, d nfsadmin.ExportDebugResp) error {
	status := "not registered"
	if d.Registered {
		mode := "rw"
		if d.ReadOnly {
			mode = "ro"
		}
		status = fmt.Sprintf("registered (%s, fsid=%d.%d, gen=%d)", mode, d.FsidMajor, d.FsidMinor, d.Generation)
	}
	if _, err := fmt.Fprintf(w, "Bucket: %s\nExport: %s\n", d.Bucket, status); err != nil {
		return err
	}
	backend := "missing"
	if d.BackendBucket.Exists {
		backend = fmt.Sprintf("exists (%d objects)", d.BackendBucket.ObjectCount)
	}
	if _, err := fmt.Fprintf(w, "Backend: %s\n", backend); err != nil {
		return err
	}
	if d.Propagation.TotalNodes > 0 {
		if _, err := fmt.Fprintf(w, "Propagation: %d/%d nodes applied\n", len(d.Propagation.AppliedNodes), d.Propagation.TotalNodes); err != nil {
			return err
		}
	}
	if len(d.ActiveMountClients) == 0 {
		if _, err := fmt.Fprintln(w, "Clients: none tracked"); err != nil {
			return err
		}
	} else if _, err := fmt.Fprintf(w, "Clients: %v\n", d.ActiveMountClients); err != nil {
		return err
	}
	if len(d.RecentLookups) == 0 {
		_, err := fmt.Fprintln(w, "Recent LOOKUPs: none")
		return err
	}
	if _, err := fmt.Fprintln(w, "Recent LOOKUPs:"); err != nil {
		return err
	}
	for _, rec := range d.RecentLookups {
		if _, err := fmt.Fprintf(w, "  - %s %s %s\n", rec.Client, rec.Bucket, rec.Result); err != nil {
			return err
		}
	}
	return nil
}
