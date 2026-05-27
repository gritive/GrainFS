// §5 T46: default bucket anonymous-access startup banner.
package server

import (
	"fmt"
	"io"
)

// EmitBanner writes the default-bucket anonymous-access startup warning to w.
// alreadyEmitted suppresses the write so callers that
// may invoke this on a replay path (e.g. snapshot Restore) can guard against
// duplicate emission without bespoke per-call-site de-dup logic.
//
// The message names the corrective action so the operator can resolve the
// warning without consulting docs.
func EmitBanner(w io.Writer, alreadyEmitted bool) {
	if alreadyEmitted {
		return
	}
	fmt.Fprintln(w, "WARN: default bucket anonymous access — any client can read/write s3://default until an explicit bucket policy is installed.")
}
