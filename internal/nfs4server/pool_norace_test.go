//go:build !race

package nfs4server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// AllocsPerRun tests are excluded under -race because the race detector's
// shadow memory instrumentation causes additional heap escapes, making the
// measured alloc count higher than the non-instrumented baseline.

func TestCompound_PUTROOTFH_GETATTR_AllocsPerRun(t *testing.T) {
	// Empty tag: no tag-string allocs. Tests the Into hot path.
	data := buildCompoundXDR("",
		opXDRPutRootFH,
		opXDRGetAttr,
	)

	srv := &Server{state: NewStateManager()}

	// Warm up pools.
	for i := 0; i < 10; i++ {
		w := getXDRWriter()
		srv.handleCompoundInto(data, w)
		putXDRWriter(w)
	}

	allocs := testing.AllocsPerRun(100, func() {
		w := getXDRWriter()
		srv.handleCompoundInto(data, w)
		putXDRWriter(w)
	})
	// PUTROOTFH+GETATTR: attrvals + result buffers only. Bitmap args use the fixed-size op pool.
	assert.LessOrEqual(t, allocs, 2.0,
		"COMPOUND PUTROOTFH+GETATTR round-trip should allocate ≤2 (got %.1f)", allocs)
}

func TestCompound_PUTROOTFH_READDIR_AllocsPerRun(t *testing.T) {
	data := buildCompoundXDR("",
		opXDRPutRootFH,
		opXDRReadDir,
	)

	srv := &Server{state: NewStateManager()}

	for i := 0; i < 10; i++ {
		w := getXDRWriter()
		srv.handleCompoundInto(data, w)
		putXDRWriter(w)
	}

	allocs := testing.AllocsPerRun(100, func() {
		w := getXDRWriter()
		srv.handleCompoundInto(data, w)
		putXDRWriter(w)
	})
	// PUTROOTFH+READDIR: only unavoidable alloc is ReadDir arg encoding (xdrWriterBytes). Target ≤3.
	assert.LessOrEqual(t, allocs, 3.0,
		"COMPOUND PUTROOTFH+READDIR round-trip should allocate ≤3 (got %.1f)", allocs)
}
