package admincli

import (
	"encoding/json"
	"io"
)

// PrintJSON encodes v as indented JSON to w. Exposed for non-volume admin
// CLI commands (dashboard, bucket scrub) so they share one canonical
// JSON-output style.
func PrintJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
