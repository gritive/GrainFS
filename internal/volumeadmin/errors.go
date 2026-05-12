package volumeadmin

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/gritive/GrainFS/internal/adminapi"
)

// Error is the typed error returned on non-2xx admin responses.
// Aliased to adminapi.Error so transport-level handling is shared with
// clusteradmin while volumeadmin keeps endpoint-specific Detail views.
type Error = adminapi.Error

// IsCode reports whether err is a *Error with the given code.
func IsCode(err error, code string) bool { return adminapi.IsCode(err, code) }

// DeleteConflictDetails is the typed view of `details` on a 409 conflict from
// DELETE /v1/volumes/<name> (no --force, snapshots present).
type DeleteConflictDetails struct {
	SnapshotCount  int                 `json:"snapshot_count"`
	Recent         []DeleteRecentEntry `json:"recent"`
	CascadeCommand string              `json:"cascade_command"`
	ListCommand    string              `json:"list_command"`
}

// DeleteRecentEntry is one row of the conflict's recent-snapshot summary.
type DeleteRecentEntry struct {
	ID         string `json:"id"`
	CreatedAt  string `json:"created_at"`
	BlockCount int64  `json:"block_count"`
}

// AsDeleteConflict returns typed details if e is a delete-conflict error.
// Best-effort: returns nil if the JSON shape doesn't match.
func AsDeleteConflict(e *Error) *DeleteConflictDetails {
	if e == nil || e.Code != "conflict" || len(e.Details) == 0 {
		return nil
	}
	var d DeleteConflictDetails
	if err := json.Unmarshal(e.Details, &d); err != nil {
		return nil
	}
	return &d
}

// ResizeUnsupportedDetails is the typed view of `details` on the 4xx
// "unsupported" returned when shrinking a volume.
type ResizeUnsupportedDetails struct {
	CurrentSize  int64  `json:"current_size"`
	Requested    int64  `json:"requested"`
	Hint         string `json:"hint"`
	CloneCommand string `json:"clone_command"`
}

// AsResizeUnsupported returns typed details if e is a shrink-unsupported error.
func AsResizeUnsupported(e *Error) *ResizeUnsupportedDetails {
	if e == nil || e.Code != "unsupported" || len(e.Details) == 0 {
		return nil
	}
	var d ResizeUnsupportedDetails
	if err := json.Unmarshal(e.Details, &d); err != nil {
		return nil
	}
	return &d
}

// FormatDeleteConflict writes a human-friendly description of a delete
// conflict to w. Falls back to the generic message if Details are absent.
func FormatDeleteConflict(w io.Writer, e *Error) {
	if e == nil {
		return
	}
	fmt.Fprintln(w, e.Message)
	d := AsDeleteConflict(e)
	if d == nil {
		return
	}
	if len(d.Recent) > 0 {
		fmt.Fprintln(w, "  Recent snapshots:")
		for _, r := range d.Recent {
			fmt.Fprintf(w, "    %s  %s  blocks=%d\n", r.ID, r.CreatedAt, r.BlockCount)
		}
	}
	if d.CascadeCommand != "" {
		fmt.Fprintf(w, "  Cascade:  %s\n", d.CascadeCommand)
	}
	if d.ListCommand != "" {
		fmt.Fprintf(w, "  Or list:  %s\n", d.ListCommand)
	}
}

// FormatResizeUnsupported writes the resize-shrink hint to w.
func FormatResizeUnsupported(w io.Writer, e *Error) {
	if e == nil {
		return
	}
	fmt.Fprintln(w, e.Message)
	d := AsResizeUnsupported(e)
	if d == nil {
		return
	}
	if d.Hint != "" {
		fmt.Fprintf(w, "  Hint:  %s\n", d.Hint)
	}
	if d.CloneCommand != "" {
		fmt.Fprintf(w, "  Try:   %s\n", d.CloneCommand)
	}
}
