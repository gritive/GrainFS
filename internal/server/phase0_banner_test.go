// §5 T46: Phase 0 anonymous banner tests.
//
// The banner is an operator-facing stdout advisory: when GrainFS boots with
// iam.anon-enabled=true, it announces that anyone can read/write s3://default
// so the operator is not surprised by the default-open posture. When the
// operator later flips anon off, a one-shot INFO banner reminds them that the
// default bucket remains public unless they install a bucket policy.
package server

import (
	"bytes"
	"strings"
	"testing"
)

// TestPhase0Banner_EmitsOnAnonStartup — spec test: anonEnabled=true → output
// contains "Phase 0 anonymous".
func TestPhase0Banner_EmitsOnAnonStartup(t *testing.T) {
	var buf bytes.Buffer
	EmitBanner(&buf, true, false)
	if !strings.Contains(buf.String(), "Phase 0 anonymous") {
		t.Fatalf("expected banner to contain 'Phase 0 anonymous', got: %q", buf.String())
	}
}

// TestPhase0Banner_SuppressedWhenAnonOff — spec test: anonEnabled=false →
// no banner emitted (zero bytes written).
func TestPhase0Banner_SuppressedWhenAnonOff(t *testing.T) {
	var buf bytes.Buffer
	EmitBanner(&buf, false, false)
	if buf.Len() != 0 {
		t.Fatalf("expected no output when anon disabled, got: %q", buf.String())
	}
}

// TestPhase0Banner_AlreadyEmittedSuppresses — alreadyEmitted=true acts as a
// one-shot guard for callers that drive EmitBanner from a path that may fire
// more than once (e.g. snapshot replay). The spec body shows the parameter
// passed but unused; we honor it so callers can rely on the guard without
// duplicating de-dup logic at every call site.
func TestPhase0Banner_AlreadyEmittedSuppresses(t *testing.T) {
	var buf bytes.Buffer
	EmitBanner(&buf, true, true)
	if buf.Len() != 0 {
		t.Fatalf("expected no output when alreadyEmitted=true, got: %q", buf.String())
	}
}

// TestEmitAnonDisabledBanner — one-shot INFO message printed when anon flips
// true→false: reminds the operator that s3://default remains public.
func TestEmitAnonDisabledBanner(t *testing.T) {
	var buf bytes.Buffer
	EmitAnonDisabledBanner(&buf)
	out := buf.String()
	if !strings.Contains(out, "s3://default") {
		t.Fatalf("expected 's3://default' in disabled banner, got: %q", out)
	}
	if !strings.Contains(out, "remains public") {
		t.Fatalf("expected 'remains public' in disabled banner, got: %q", out)
	}
}

// TestEmitBanner_WriterIsRespected — writes go to the supplied io.Writer, not
// stdout. Guards against a regression where EmitBanner hard-codes os.Stdout.
func TestEmitBanner_WriterIsRespected(t *testing.T) {
	var buf bytes.Buffer
	EmitBanner(&buf, true, false)
	if buf.Len() == 0 {
		t.Fatalf("expected EmitBanner to write to supplied buffer")
	}
}
