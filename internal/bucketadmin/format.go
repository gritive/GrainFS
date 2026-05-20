// Package bucketadmin text-mode renderers. These reproduce the human-friendly
// output of legacy cmd/grainfs/bucket.go and bucket_versioning.go byte-for-byte.
// JSON-mode output is handled by the caller via WriteRawWithNewline.
package bucketadmin

import (
	"fmt"
	"io"
	"strconv"
	"text/tabwriter"
)

// tabwriter params used by every table renderer (identical to legacy cmd).
const (
	twMinwidth = 0
	twTabwidth = 0
	twPadding  = 2
	twPadchar  = ' '
	twFlags    = 0
)

// RenderCreateText mirrors legacy `grainfs bucket create` text-mode output.
func RenderCreateText(w io.Writer, name string) {
	fmt.Fprintf(w, "Created bucket %s\n", name)
}

// RenderListText mirrors legacy `grainfs bucket list` text-mode output.
func RenderListText(w io.Writer, items []ListItem) error {
	tw := tabwriter.NewWriter(w, twMinwidth, twTabwidth, twPadding, twPadchar, twFlags)
	fmt.Fprintln(tw, "NAME\tHAS_UPSTREAM")
	for _, b := range items {
		upstream := "no"
		if b.HasUpstream {
			upstream = "yes"
		}
		fmt.Fprintf(tw, "%s\t%s\n", b.Name, upstream)
	}
	return tw.Flush()
}

// RenderDeleteText mirrors legacy `grainfs bucket delete` text-mode output.
func RenderDeleteText(w io.Writer, name string) {
	fmt.Fprintf(w, "Deleted bucket %s\n", name)
}

// RenderInfoText mirrors legacy `grainfs bucket info` text-mode output.
// Conditional fields: ObjectCount==nil → "<unknown>"; empty Versioning → "-";
// HasUpstream → "yes"/"no".
func RenderInfoText(w io.Writer, r InfoResponse) error {
	tw := tabwriter.NewWriter(w, twMinwidth, twTabwidth, twPadding, twPadchar, twFlags)
	fmt.Fprintln(tw, "NAME\tOBJECTS\tVERSIONING\tHAS_UPSTREAM")
	objects := "<unknown>"
	if r.ObjectCount != nil {
		objects = strconv.FormatInt(*r.ObjectCount, 10)
	}
	versioning := "-"
	if r.Versioning != "" {
		versioning = r.Versioning
	}
	upstream := "no"
	if r.HasUpstream {
		upstream = "yes"
	}
	fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", r.Name, objects, versioning, upstream)
	return tw.Flush()
}

// RenderVersioningGetText mirrors legacy `grainfs bucket versioning get`.
// Empty status renders as "Off" (legacy behaviour).
func RenderVersioningGetText(w io.Writer, bucket string, status VersioningStatus) error {
	s := status.Status
	if s == "" {
		s = "Off"
	}
	tw := tabwriter.NewWriter(w, twMinwidth, twTabwidth, twPadding, twPadchar, twFlags)
	fmt.Fprintln(tw, "BUCKET\tVERSIONING")
	fmt.Fprintf(tw, "%s\t%s\n", bucket, s)
	return tw.Flush()
}

// RenderVersioningEnableText mirrors legacy `grainfs bucket versioning enable`.
func RenderVersioningEnableText(w io.Writer, bucket string) {
	fmt.Fprintf(w, "Versioning enabled for bucket %s\n", bucket)
}

// RenderVersioningSuspendText mirrors legacy `grainfs bucket versioning suspend`.
func RenderVersioningSuspendText(w io.Writer, bucket string) {
	fmt.Fprintf(w, "Versioning suspended for bucket %s\n", bucket)
}

// WriteRawWithNewline writes a JSON body verbatim followed by a newline —
// the byte-for-byte equivalent of `fmt.Fprintln(w, string(body))` used by
// every --json passthrough in the legacy cmd code.
func WriteRawWithNewline(w io.Writer, body []byte) {
	fmt.Fprintln(w, string(body))
}
