package volumeadmin

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Error is the typed error returned by Client and Run* on non-2xx responses
// from the admin server. Status mirrors HTTP; Code mirrors the admin error
// JSON envelope; Details is the raw JSON-decoded detail map.
type Error struct {
	Code    string         `json:"code"`
	Message string         `json:"error"`
	Details map[string]any `json:"details,omitempty"`
	Status  int            `json:"-"`

	// cause carries the wrapped transport-level error (e.g. context.Canceled)
	// so errors.Is / errors.As can still see through the typed envelope.
	cause error `json:"-"`
}

// Error implements error. Returns Message if set, otherwise Code.
func (e *Error) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return e.Code
}

// Unwrap exposes a wrapped underlying error so callers can use errors.Is /
// errors.As to inspect e.g. context.Canceled / context.DeadlineExceeded
// while still receiving a typed *Error envelope.
func (e *Error) Unwrap() error { return e.cause }

// IsCode reports whether err is a *Error with the given code. Convenience
// over errors.As when callers only need the Code branch.
func IsCode(err error, code string) bool {
	var e *Error
	if !errors.As(err, &e) {
		return false
	}
	return e.Code == code
}

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
func (e *Error) AsDeleteConflict() *DeleteConflictDetails {
	if e == nil || e.Code != "conflict" || e.Details == nil {
		return nil
	}
	var d DeleteConflictDetails
	if err := remapJSON(e.Details, &d); err != nil {
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
func (e *Error) AsResizeUnsupported() *ResizeUnsupportedDetails {
	if e == nil || e.Code != "unsupported" || e.Details == nil {
		return nil
	}
	var d ResizeUnsupportedDetails
	if err := remapJSON(e.Details, &d); err != nil {
		return nil
	}
	return &d
}

// remapJSON re-encodes a generic JSON map into a typed struct. Used to
// materialize typed Detail views from the loose Details map.
func remapJSON(in map[string]any, out any) error {
	buf, err := json.Marshal(in)
	if err != nil {
		return err
	}
	return json.Unmarshal(buf, out)
}

// FormatDeleteConflict writes a human-friendly description of a delete
// conflict to w. Falls back to the generic message if Details are absent.
func FormatDeleteConflict(w io.Writer, e *Error) {
	if e == nil {
		return
	}
	fmt.Fprintln(w, e.Message)
	d := e.AsDeleteConflict()
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
	d := e.AsResizeUnsupported()
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
