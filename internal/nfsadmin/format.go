package nfsadmin

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"text/tabwriter"
)

func RenderExportList(w io.Writer, jsonOut bool, exports []NfsExportInfo) error {
	if jsonOut {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(ListNfsExportsResp{Exports: exports})
	}
	if len(exports) == 0 {
		_, err := fmt.Fprintln(w, "(no NFS exports)")
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "BUCKET\tMODE\tFSID\tGEN")
	for _, e := range exports {
		mode := "rw"
		if e.ReadOnly {
			mode = "ro"
		}
		fmt.Fprintf(tw, "%s\t%s\t%d.%d\t%d\n", e.Bucket, mode, e.FsidMajor, e.FsidMinor, e.Generation)
	}
	return tw.Flush()
}

func RenderExportDebug(w io.Writer, d ExportDebugResp) error {
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
	if d.ActiveMountClients == nil {
		if _, err := fmt.Fprintln(w, "Clients: tracking not enabled"); err != nil {
			return err
		}
	} else if len(d.ActiveMountClients) == 0 {
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

func RenderError(w io.Writer, err error) {
	var e *Error
	if !errors.As(err, &e) {
		fmt.Fprintf(w, "Error: %v\n", err)
		return
	}
	fmt.Fprintf(w, "Error: %s (%s)\n", e.Message, e.Code)
	if e.Help != "" {
		fmt.Fprintf(w, "Hint:  %s\n", e.Help)
	}
	if e.DocsURL != "" {
		fmt.Fprintf(w, "Docs:  %s\n", e.DocsURL)
	}
}
