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
