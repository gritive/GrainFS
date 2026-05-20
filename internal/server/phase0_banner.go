// §5 T46: Phase 0 anonymous-access startup banner.
//
// GrainFS ships with iam.anon-enabled=true by default — anyone who can reach
// the data port can read/write s3://default. This is the right out-of-the-box
// experience (zero-config dev clusters work immediately), but it is also the
// most operationally surprising default in the product. The banner exists to
// make sure no operator misses it on first boot.
//
// EmitBanner runs once at startup from the serveruntime boot phase
// (bootPhase0Banner). It is a stdout text emission, NOT enforcement — turning
// anon off is a separate config flip gated by the §5 T44 TLS posture check.
//
// EmitAnonDisabledBanner runs at most once per process: when an operator flips
// iam.anon-enabled from true→false via `grainfs config set`, it reminds them
// that s3://default itself remains public until they install a bucket policy.
// The transition is detected by the serveruntime hook in
// internal/serveruntime/phase0_banner.go using an atomic.Bool snapshot.
package server

import (
	"fmt"
	"io"
)

// EmitBanner writes the Phase 0 anonymous-mode startup warning to w when
// anonEnabled is true. alreadyEmitted suppresses the write so callers that
// may invoke this on a replay path (e.g. snapshot Restore) can guard against
// duplicate emission without bespoke per-call-site de-dup logic.
//
// The message names the corrective action (create an SA via the admin UDS) so
// the operator can resolve the warning without consulting docs.
func EmitBanner(w io.Writer, anonEnabled, alreadyEmitted bool) {
	if !anonEnabled || alreadyEmitted {
		return
	}
	fmt.Fprintln(w, "WARN: Phase 0 anonymous access — any client can read/write s3://default. "+
		"Run 'grainfs iam sa create <name>' to require auth on other buckets.")
}

// EmitAnonDisabledBanner writes the one-shot INFO message reminding the
// operator that s3://default remains world-readable/writable after anon is
// flipped off. Called from the OnAnonEnabledChange hook on a true→false
// transition (see internal/serveruntime/phase0_banner.go).
func EmitAnonDisabledBanner(w io.Writer) {
	fmt.Fprintln(w, "INFO: 's3://default' remains public. Override with 'grainfs iam bucket policy put default ...'.")
}
