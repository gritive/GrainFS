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

// ResizeUnsupportedDetails is the typed view of `details` on the 4xx
// "unsupported" returned when shrinking a volume.
type ResizeUnsupportedDetails struct {
	CurrentSize int64  `json:"current_size"`
	Requested   int64  `json:"requested"`
	Hint        string `json:"hint"`
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
}
