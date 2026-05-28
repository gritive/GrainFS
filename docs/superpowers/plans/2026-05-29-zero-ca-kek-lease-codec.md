# Zero-CA KEK Lease Codec Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Harden the KEK lease snapshot probe wire codec by refusing oversized `node_id` values during encode and rejecting trailing bytes during decode.

**Architecture:** Keep the existing simple binary wire format and add explicit validation at the codec boundary. Encoding becomes fallible only for the response path because `node_id_len` is a `uint16`; callers that publish responses must propagate encode failure as a transport error. Decoding remains fail-closed and now requires the payload length to match the declared `node_id_len` exactly.

**Tech Stack:** Go, `encoding/binary`, existing `internal/cluster` transport message helpers, table-driven unit tests.

---

## File Structure

- Modify `internal/cluster/kek_lease_rpc.go`: make response encoding return `([]byte, error)`, guard `NodeID` length, reject trailing decode bytes, and propagate handler encode errors.
- Modify `internal/cluster/kek_lease_rpc_test.go`: add regression tests for oversized encode and trailing decode bytes; update existing round-trip tests for the new encode signature.
- Modify `internal/cluster/kek_peer_probe_production_test.go`: update fake response encoders for the new encode signature.
- Modify `TODOS.md`: remove the completed KEK lease-probe wire codec follow-up.

## Task 1: Codec Tests

**Files:**
- Modify: `internal/cluster/kek_lease_rpc_test.go`

- [ ] **Step 1: Add failing encode/decode regression tests**

Add these tests near the existing response magic tests:

```go
func TestKEKLeaseRPC_ResponseEncodeRejectsOversizedNodeID(t *testing.T) {
	_, err := encodeKEKLeaseSnapshotResp(KEKLeaseSnapshotResp{
		NodeID: strings.Repeat("n", math.MaxUint16+1),
	})
	if err == nil {
		t.Fatal("encodeKEKLeaseSnapshotResp accepted node_id longer than uint16")
	}
}

func TestKEKLeaseRPC_ResponseDecodeRejectsTrailingBytes(t *testing.T) {
	payload, err := encodeKEKLeaseSnapshotResp(KEKLeaseSnapshotResp{
		LeaseCount:                1,
		ObservedAtRaftCommitIndex: 2,
		SnapshotRefCount:          3,
		NodeID:                    "node-a",
	})
	if err != nil {
		t.Fatalf("encodeKEKLeaseSnapshotResp: %v", err)
	}
	payload = append(payload, 0xff)
	_, err = decodeKEKLeaseSnapshotResp(payload)
	if err == nil {
		t.Fatal("decodeKEKLeaseSnapshotResp accepted trailing bytes")
	}
}
```

Also add imports:

```go
import (
	"math"
	"strings"
)
```

- [ ] **Step 2: Run focused tests and verify failure**

Run:

```bash
go test ./internal/cluster -run 'TestKEKLeaseRPC_Response(EncodeRejectsOversizedNodeID|DecodeRejectsTrailingBytes)' -count=1
```

Expected before implementation: compile failure or test failure because `encodeKEKLeaseSnapshotResp` still returns one value and accepts both invalid cases.

## Task 2: Minimal Codec Hardening

**Files:**
- Modify: `internal/cluster/kek_lease_rpc.go`
- Modify: `internal/cluster/kek_lease_rpc_test.go`
- Modify: `internal/cluster/kek_peer_probe_production_test.go`

- [ ] **Step 1: Change response encoder signature and length guard**

Replace `encodeKEKLeaseSnapshotResp` with:

```go
func encodeKEKLeaseSnapshotResp(resp KEKLeaseSnapshotResp) ([]byte, error) {
	idLen := len(resp.NodeID)
	if idLen > math.MaxUint16 {
		return nil, fmt.Errorf("kek_lease_snapshot_probe: node_id too long: %d > %d", idLen, math.MaxUint16)
	}
	out := make([]byte, 0, len(kekLeaseSnapshotRespMagic)+8+8+8+2+idLen)
	out = append(out, kekLeaseSnapshotRespMagic...)
	out = binary.BigEndian.AppendUint64(out, resp.LeaseCount)
	out = binary.BigEndian.AppendUint64(out, resp.ObservedAtRaftCommitIndex)
	out = binary.BigEndian.AppendUint64(out, resp.SnapshotRefCount)
	out = binary.BigEndian.AppendUint16(out, uint16(idLen))
	out = append(out, resp.NodeID...)
	return out, nil
}
```

Add `math` to `internal/cluster/kek_lease_rpc.go` imports.

- [ ] **Step 2: Propagate handler encode errors**

In `KEKLeaseSnapshotHandler.Handle`, replace the final return with:

```go
	payload, err := encodeKEKLeaseSnapshotResp(resp)
	if err != nil {
		return transport.NewErrorResponse(req, transport.StatusError,
			fmt.Errorf("kek_lease_snapshot_probe: encode response: %w", err))
	}
	return transport.NewResponse(req, payload)
```

- [ ] **Step 3: Reject exact-length decode mismatches**

After the existing truncated-node check in `decodeKEKLeaseSnapshotResp`, add:

```go
	if len(rest) != idLen {
		return KEKLeaseSnapshotResp{}, fmt.Errorf("kek_lease_snapshot_probe: response has trailing bytes after node_id (node_id_len %d, trailing %d)", idLen, len(rest)-idLen)
	}
```

- [ ] **Step 4: Update tests and fake callers for the new encode signature**

For each existing test call that directly uses `encodeKEKLeaseSnapshotResp`, capture and check the error:

```go
	payload, err := encodeKEKLeaseSnapshotResp(resp)
	if err != nil {
		t.Fatalf("encodeKEKLeaseSnapshotResp: %v", err)
	}
	return payload, nil
```

For `KEKLeaseSnapshotHandler.Handle` round trips, no call-site change is needed beyond decode assertions because the handler now returns an error response if encoding fails.

- [ ] **Step 5: Run focused tests**

Run:

```bash
go test ./internal/cluster -run 'TestKEKLeaseRPC|TestProductionPeerKEKProbe_LeaseSnapshot' -count=1
```

Expected: PASS.

## Task 3: TODO Tracking And Verification

**Files:**
- Modify: `TODOS.md`

- [ ] **Step 1: Remove the completed follow-up**

Delete this completed entry from `TODOS.md`:

```markdown
- [ ] **KEK lease-probe wire codec: bound node_id length [P3]**.
```

Remove its wrapped explanatory lines as well. Completed items do not stay in `TODOS.md`.

- [ ] **Step 2: Run package verification**

Run:

```bash
go test ./internal/cluster -run 'TestKEKLeaseRPC|TestProductionPeerKEKProbe_LeaseSnapshot|TestLeaderProposeKEKPrune|TestFSM_Apply_KEKPrune' -count=1
```

Expected: PASS.

- [ ] **Step 3: Run gofmt**

Run:

```bash
gofmt -w internal/cluster/kek_lease_rpc.go internal/cluster/kek_lease_rpc_test.go internal/cluster/kek_peer_probe_production_test.go
```

- [ ] **Step 4: Check diff**

Run:

```bash
git diff -- internal/cluster/kek_lease_rpc.go internal/cluster/kek_lease_rpc_test.go internal/cluster/kek_peer_probe_production_test.go TODOS.md
```

Expected: only codec hardening, tests, and TODO completion are changed.

## GSTACK REVIEW REPORT

Plan self-review:
- Spec coverage: covers both TODO requirements: response encoder bounds `node_id` before uint16 cast, and response decoder rejects trailing bytes.
- Placeholder scan: no placeholder implementation steps remain.
- Type consistency: all code uses existing `KEKLeaseSnapshotResp`, `encodeKEKLeaseSnapshotResp`, `decodeKEKLeaseSnapshotResp`, and test package conventions.
