package credentialadmin

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
)

func printJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func RenderCredential(w io.Writer, c Credential) {
	fmt.Fprintf(w, "id:              %s\n", c.ID)
	fmt.Fprintf(w, "sa_id:           %s\n", c.SAID)
	fmt.Fprintf(w, "protocol:        %s\n", c.Protocol)
	fmt.Fprintf(w, "resource:        %s\n", c.Resource)
	fmt.Fprintf(w, "mode:            %s\n", c.Mode)
	if c.Secret != "" {
		fmt.Fprintf(w, "secret:          %s\n", c.Secret)
	}
	if c.SecretHint != "" {
		fmt.Fprintf(w, "secret_hint:     %s\n", c.SecretHint)
	}
	if len(c.ConnectionHint) > 0 {
		fmt.Fprintf(w, "connection_hint: %s\n", formatConnectionHint(c.ConnectionHint))
	}
	if c.ExpiresAt != "" {
		fmt.Fprintf(w, "expires_at:      %s\n", c.ExpiresAt)
	}
	if c.RevokedAt != "" {
		fmt.Fprintf(w, "revoked_at:      %s\n", c.RevokedAt)
	}
}

func RenderList(w io.Writer, items []Credential) {
	if len(items) == 0 {
		fmt.Fprintln(w, "(no credentials)")
		return
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tSA ID\tPROTOCOL\tRESOURCE\tMODE\tEXPIRES\tREVOKED")
	for _, c := range items {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			c.ID, c.SAID, c.Protocol, c.Resource, c.Mode, emptyDash(c.ExpiresAt), emptyDash(c.RevokedAt))
	}
	_ = tw.Flush()
}

func formatConnectionHint(h map[string]string) string {
	parts := make([]string, 0, len(h))
	keys := []string{"export_name", "mount_path", "aname", "access_key_id", "client_id"}
	seen := map[string]bool{}
	for _, k := range keys {
		if v, ok := h[k]; ok {
			parts = append(parts, k+"="+v)
			seen[k] = true
		}
	}
	for k, v := range h {
		if !seen[k] {
			parts = append(parts, k+"="+v)
		}
	}
	return strings.Join(parts, " ")
}

func emptyDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
