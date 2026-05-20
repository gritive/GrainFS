package iamadmin

import (
	"fmt"
	"io"
	"text/tabwriter"
	"time"
)

// tabwriter parameters preserved from cmd/grainfs/iam.go (minwidth=0,
// tabwidth=0, padding=2, padchar=' ', flags=0). Changing these shifts
// every column and breaks the public CLI output contract.

// RenderSACreateText prints the human-friendly summary of a freshly
// created ServiceAccount: greeting line, key/secret table, and the
// one-time secret warning. Matches the legacy cmd/grainfs/iam.go layout.
func RenderSACreateText(w io.Writer, r SACreateResponse) {
	fmt.Fprintf(w, "Created service account %s\n", r.Name)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "sa_id:\t%s\n", r.SAID)
	fmt.Fprintf(tw, "access_key:\t%s\n", r.AccessKey)
	fmt.Fprintf(tw, "secret_key:\t%s\n", r.SecretKey)
	_ = tw.Flush()
	fmt.Fprintln(w, "Store the secret_key now — it will not be shown again.")
}

// RenderSAListText prints a tabular listing of ServiceAccounts.
func RenderSAListText(w io.Writer, items []SAListItem) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "SA ID\tNAME\tKEYS\tGRANTS\tCREATED AT")
	for _, sa := range items {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%s\n",
			sa.SAID, sa.Name, sa.NumKeys, sa.NumGrants,
			sa.CreatedAt.Format(time.RFC3339))
	}
	_ = tw.Flush()
}

// RenderSAGetText prints a single-row table for one ServiceAccount.
func RenderSAGetText(w io.Writer, r SAGetResponse) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "SA ID\tNAME\tDESCRIPTION\tCREATED AT")
	fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
		r.SAID, r.Name, r.Description, r.CreatedAt.Format(time.RFC3339))
	_ = tw.Flush()
}

// RenderSADeleteText prints the deletion-feedback line.
func RenderSADeleteText(w io.Writer, saID string) {
	fmt.Fprintf(w, "Deleted service account %s\n", saID)
}

// WriteRawWithNewline writes body verbatim followed by a single '\n',
// matching the existing `fmt.Fprintln(out, string(body))` semantic in
// cmd/iam.go's --json paths and the key/grant passthrough commands.
// Preserves whatever spacing/key-order the server emits.
func WriteRawWithNewline(w io.Writer, body []byte) {
	fmt.Fprintln(w, string(body))
}
