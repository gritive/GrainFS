package e2e

import (
	"github.com/onsi/ginkgo/v2"
)

// NFSv4 protocol ops dispatched by internal/nfs4server but lacking explicit
// e2e coverage. Wire-level unit tests in internal/nfs4server/*_test.go
// already exercise these in-process; the gap here is *explicit e2e* talking
// to the running grainfs NFSv4 TCP port without depending on a kernel mount.
// Some of these (ExchangeID, CreateSession, Sequence) are implicitly invoked
// during colima kernel-mount tests; PIt entries capture the still-missing
// dedicated wire-level e2e.
var _ = ginkgo.Describe("NFSv4 protocol op skeletons", func() {
	for _, tc := range []struct {
		name string
		mk   func() *nfsTarget
	}{
		{name: "SingleNode", mk: func() *nfsTarget { return newSingleNodeNFSTarget(ginkgo.GinkgoTB()) }},
		{name: "Cluster4Node", mk: func() *nfsTarget { return newSharedClusterNFSTarget(ginkgo.GinkgoTB()) }},
	} {
		tc := tc
		ginkgo.Context(tc.name, func() {
			var tgt *nfsTarget
			_ = tgt

			ginkgo.BeforeEach(func() {
				tgt = tc.mk()
			})

			runNFS4ProtocolOpSkeletonCases(func() *nfsTarget { return tgt })
		})
	}
})

func runNFS4ProtocolOpSkeletonCases(getTgt func() *nfsTarget) {
	_ = getTgt

	// NFSv4.0 ops with only compound/wire unit coverage today.
	ginkgo.PIt("[TODO:e2e] OP_SAVEFH preserves current filehandle across COMPOUND ops", func() {
		// Issue COMPOUND { PUTROOTFH; LOOKUP; SAVEFH; LOOKUP; RESTOREFH; GETFH }
		// and verify final GETFH matches the saved handle.
	})

	ginkgo.PIt("[TODO:e2e] OP_RESTOREFH restores a SAVEFH-captured filehandle", func() {
		// Paired with the SAVEFH case above; verify restore is exact and
		// subsequent ops dispatch against the restored handle.
	})

	// NFSv4.1 session/state ops — implicitly exercised by colima mount but
	// missing dedicated wire-level e2e.
	ginkgo.PIt("[TODO:e2e] OP_EXCHANGE_ID establishes a client identity", func() {
		// Send EXCHANGE_ID; verify ClientID + sequenceID returned and that
		// re-issuing with same owner returns equivalent ClientID.
	})

	ginkgo.PIt("[TODO:e2e] OP_CREATE_SESSION binds a session to an EXCHANGE_ID", func() {
		// Verify slot table, max_request_size, and that subsequent OPs gated
		// by the session succeed only after CREATE_SESSION.
	})

	ginkgo.PIt("[TODO:e2e] OP_DESTROY_SESSION tears down the session cleanly", func() {
		// After DESTROY_SESSION, follow-up OPs on the sessionID must fail
		// with NFS4ERR_BADSESSION.
	})

	ginkgo.PIt("[TODO:e2e] OP_FREE_STATEID releases a state identifier", func() {
		// Issue OPEN to acquire a stateid, then FREE_STATEID, then OPEN_DOWNGRADE
		// or READ with that stateid must fail with NFS4ERR_BAD_STATEID.
	})

	ginkgo.PIt("[TODO:e2e] OP_SEQUENCE advances the slot table sequence number", func() {
		// Verify duplicate sequence numbers in same slot are detected;
		// out-of-window numbers rejected.
	})

	ginkgo.PIt("[TODO:e2e] OP_TEST_STATEID reports stateid liveness", func() {
		// Submit a known-live stateid and a freed stateid; verify
		// per-stateid status codes in the response array.
	})

	ginkgo.PIt("[TODO:e2e] OP_DESTROY_CLIENTID terminates the client", func() {
		// After DESTROY_CLIENTID, any session under that ClientID must fail.
	})

	ginkgo.PIt("[TODO:e2e] OP_RECLAIM_COMPLETE signals end-of-grace reclaim", func() {
		// After CREATE_SESSION + RECLAIM_COMPLETE, normal (non-reclaim)
		// OPENs must be accepted; before RECLAIM_COMPLETE, server may
		// reject as appropriate per grace period semantics.
	})

	// NFSv4.2 ops.
	ginkgo.PIt("[TODO:e2e] OP_IO_ADVISE accepts advisory hints without erroring", func() {
		// Issue IO_ADVISE with WILLNEED / DONTNEED hint; verify success
		// reply and that subsequent READ is not impaired.
	})

	// Modifier-level variants — operation already wired, missing per-variant e2e.
	ginkgo.PIt("[TODO:e2e] OP_REMOVE on a directory (rmdir) removes empty dir", func() {
		// File removal is covered; directory removal path needs its own case.
	})

	ginkgo.PIt("[TODO:e2e] OP_REMOVE on a non-empty directory returns NFS4ERR_NOTEMPTY", func() {
		// Verify the dir-not-empty guard. Requires populating the dir first.
	})

	ginkgo.PIt("[TODO:e2e] OP_RENAME across directories preserves attributes", func() {
		// Same-dir rename is covered; cross-dir needs distinct source/target FH.
	})

	ginkgo.PIt("[TODO:e2e] OP_RENAME over an existing target replaces atomically", func() {
		// Verify destination is overwritten and source no longer resolvable.
	})

	ginkgo.PIt("[TODO:e2e] OP_OPEN with OPEN4_NOCREATE opens an existing file", func() {
		// Only OPEN4_CREATE is covered today. NOCREATE on missing must return NOENT.
	})

	ginkgo.PIt("[TODO:e2e] OP_OPEN with OPEN4_CREATE + truncate clears file content", func() {
		// Verify create-with-truncate semantics on existing target.
	})

	ginkgo.PIt("[TODO:e2e] OP_CREATE of type NF4DIR makes a new directory", func() {
		// Directory create path is parse-tested only; needs e2e through wire.
	})

	ginkgo.PIt("[TODO:e2e] OP_GETATTR reports SIZE correctly after write", func() {
		// Currently only TYPE/FSID/MODE asserted. Add SIZE attribute check.
	})

	ginkgo.PIt("[TODO:e2e] OP_GETATTR reports OWNER/GROUP per server policy", func() {
		// Server returns fixed owner under D#8; assert that explicitly.
	})

	ginkgo.PIt("[TODO:e2e] OP_GETATTR reports MTIME/ATIME/CTIME consistently", func() {
		// Verify timestamps move forward across WRITE and SETATTR.
	})

	ginkgo.PIt("[TODO:e2e] OP_READDIR on an empty directory returns no entries", func() {
		// Empty dir path is implicit today; needs explicit assertion.
	})

	ginkgo.PIt("[TODO:e2e] OP_READDIR with cookie continuation paginates correctly", func() {
		// Large directory crosses a single READDIR; verify cookie-driven resume.
	})

	ginkgo.PIt("[TODO:e2e] OP_READDIR with stale cookie/verifier returns NFS4ERR_BAD_COOKIE", func() {
		// Error path for clients with invalidated continuation tokens.
	})

	ginkgo.PIt("[TODO:e2e] OP_COMMIT respects FILE_SYNC vs DATA_SYNC stability", func() {
		// Existing test only exercises verifier; verify the two stability
		// classes produce the documented persistence guarantees.
	})

	ginkgo.PIt("[TODO:e2e] OP_COMMIT honors offset+count for partial-range commit", func() {
		// Only full-file COMMIT (0,0) is covered; partial range needs explicit case.
	})
}
