# NBD Modern Protocol Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make GrainFS NBD negotiate fixed newstyle correctly and add modern NBD features without regressing the existing Linux `nbd-client` path.

**Architecture:** Split the current monolithic NBD server into explicit negotiation state, request parsing, reply writing, and command handlers. Do not advertise an extension until the parser, handler, reply path, tests, and interop gate for that extension exist. Keep simple replies as the default and keep extended headers disabled until qemu/libnbd smoke passes.

**Tech Stack:** Go, `net.Conn`, `encoding/binary`, `testing`, `testify`, existing `volume.Manager`, Makefile, optional qemu/libnbd tooling.

---

## File Structure

- Modify `internal/nbd/nbd.go`: constants, server wiring, connection lifecycle, command dispatch.
- Create `internal/nbd/handshake.go`: fixed newstyle state, client flag validation, option parsing.
- Create `internal/nbd/request.go`: compact and experimental extended request parsing plus size/flag validation.
- Create `internal/nbd/reply.go`: simple, structured, block-status, and disabled extended reply helpers.
- Create `internal/nbd/modern_test.go`: protocol negotiation, structured reply, block-status, and size-limit tests.
- Modify `internal/nbd/nbd_test.go`: adapt helpers to the new handshake state.
- Modify `internal/nbd/nbd_bench_test.go`: allocation-aware benchmarks for modern paths.
- Modify `internal/volume/volume.go`: add a correctness-first zero-range helper only if the NBD layer cannot express `WRITE_ZEROES` through existing write/discard primitives cleanly.
- Create `tests/nbd_interop/nbd_interop_test.go`: optional qemu/libnbd smoke with clear skip output.
- Modify `Makefile`: add `test-nbd-interop`.
- Modify `README.md`: document modern NBD status and interop test command.

## Task 0: Test Helper and Constant Scaffold

**Files:**
- Create: `internal/nbd/modern_test.go`
- Modify: `internal/nbd/nbd.go`

- [x] **Step 1: Add shared protocol constants**

Add all constants used by later tasks before writing their focused tests, even if some handlers still return unsupported:

```go
const (
    nbdFlagClientFixedNewstyle = uint32(1 << 0)
    nbdFlagClientNoZeroes      = uint32(1 << 1)
    nbdKnownClientFlags        = nbdFlagClientFixedNewstyle | nbdFlagClientNoZeroes

    nbdFlagSendWriteZeroes = uint16(1 << 6)
    nbdFlagSendDF          = uint16(1 << 7)

    nbdOptInfo            = uint32(6)
    nbdOptStructuredReply = uint32(8)
    nbdOptListMetaContext = uint32(9)
    nbdOptSetMetaContext  = uint32(10)
    nbdOptExtendedHeaders = uint32(11)

    nbdRepErrInvalid = uint32(3 | (1 << 31))
    nbdRepErrUnknown = uint32(6 | (1 << 31))

    nbdInfoBlockSize = uint16(3)

    nbdCmdWriteZeroes = uint32(6)
    nbdCmdBlockStatus = uint32(7)

    nbdCmdFlagNoHole   = uint16(1 << 1)
    nbdCmdFlagDF       = uint16(1 << 2)
    nbdCmdFlagReqOne   = uint16(1 << 3)
    nbdCmdFlagFastZero = uint16(1 << 4)

    nbdMinBlockSize       = uint32(1)
    nbdPreferredBlockSize = uint32(volume.DefaultBlockSize)
    nbdMaxPayloadSize     = uint32(32 * 1024 * 1024)

    nbdStructuredReplyMagic   = uint32(0x668e33ef)
    nbdExtendedRequestMagic   = uint32(0x21e41c71)
    nbdReplyFlagDone          = uint16(1 << 0)
    nbdReplyTypeOffsetData    = uint16(1)
    nbdReplyTypeBlockStatus   = uint16(5)
    nbdReplyTypeError         = uint16(32769)
    nbdMetaContextBaseAllocID = uint32(1)

    nbdErrEIO    = uint32(5)
    nbdErrEINVAL = uint32(22)
)
```

- [x] **Step 2: Add raw negotiation and request helpers**

Create `internal/nbd/modern_test.go` with these helpers so every later test names an existing helper:

```go
type optionReply struct {
    opt     uint32
    typ     uint32
    payload []byte
}

type simpleReply struct {
    errCode uint32
    handle  [8]byte
}

type structuredChunk struct {
    flags   uint16
    typ     uint16
    handle  [8]byte
    payload []byte
}

func setupRawNBDConn(t *testing.T) (net.Conn, *Server)
func readServerHeader(t *testing.T, conn net.Conn)
func writeClientFlags(t *testing.T, conn net.Conn, flags uint32)
func completeClientFlags(t *testing.T, conn net.Conn, flags uint32)
func writeOptExportName(t *testing.T, conn net.Conn, name string)
func writeOptGo(t *testing.T, conn net.Conn, name string, info []uint16)
func writeOptInfo(t *testing.T, conn net.Conn, name string, info []uint16)
func writeEmptyOption(t *testing.T, conn net.Conn, opt uint32)
func readExact(t *testing.T, conn net.Conn, n int) []byte
func readOptionReplyHeader(conn net.Conn) (optionReply, error)
func readOptionReply(t *testing.T, conn net.Conn) optionReply
func readInfoReply(t *testing.T, conn net.Conn, infoType uint16) []byte
func readSimpleReply(t *testing.T, conn net.Conn) simpleReply
func readStructuredChunk(t *testing.T, conn net.Conn) structuredChunk
func sendRawRequest(t *testing.T, conn net.Conn, typ uint32, offset uint64, length uint64, payload []byte, flags uint16)
func sendReadRequest(t *testing.T, conn net.Conn, offset uint64, length uint32)
func sendWriteZeroesConn(t *testing.T, conn net.Conn, offset uint64, length uint32, flags uint16)
func sendTrimConn(t *testing.T, conn net.Conn, offset uint64, length uint32)
func sendBlockStatusRequest(t *testing.T, conn net.Conn, offset uint64, length uint32)
func setupStructuredNBD(t *testing.T) (net.Conn, *Server)
func setupBlockStatusNBD(t *testing.T) (net.Conn, *Server)
```

- [x] **Step 3: Run scaffold compile check**

Run:

```bash
go test ./internal/nbd -run '^$' -count=1
```

Expected: PASS after scaffold compiles.

- [x] **Step 4: Commit**

```bash
git add internal/nbd/modern_test.go internal/nbd/nbd.go
git commit -m "test: scaffold nbd modern protocol helpers"
```

## Task 1: Fixed Newstyle Foundation

**Files:**
- Create: `internal/nbd/handshake.go`
- Create: `internal/nbd/modern_test.go`
- Modify: `internal/nbd/nbd.go`
- Modify: `internal/nbd/nbd_test.go`

- [x] **Step 1: Write failing client flag tests**

Add tests to `internal/nbd/modern_test.go`:

```go
func TestNBDHandshakeRejectsUnknownClientFlags(t *testing.T) {
    client, _ := setupRawNBDConn(t)
    readServerHeader(t, client)
    writeClientFlags(t, client, 1<<31)
    _, err := readOptionReplyHeader(client)
    require.Error(t, err)
}

func TestNBDExportNameHonorsNoZeroes(t *testing.T) {
    client, _ := setupRawNBDConn(t)
    readServerHeader(t, client)
    writeClientFlags(t, client, nbdFlagClientFixedNewstyle|nbdFlagClientNoZeroes)
    writeOptExportName(t, client, "nbd-test")
    reply := readExact(t, client, 10)
    require.Equal(t, uint64(1024*1024), binary.BigEndian.Uint64(reply[0:8]))
    require.Equal(t, nbdFlagHasFlags|nbdFlagSendFlush|nbdFlagSendTrim, binary.BigEndian.Uint16(reply[8:10]))
}
```

- [x] **Step 2: Run tests and confirm failure**

Run:

```bash
go test ./internal/nbd -run 'TestNBDHandshakeRejectsUnknownClientFlags|TestNBDExportNameHonorsNoZeroes' -count=1
```

Expected: FAIL because helper functions/constants and `NO_ZEROES` behavior do not exist.

- [x] **Step 3: Implement handshake state and client flag validation**

Create `handshakeState` and constants:

```go
const (
    nbdFlagClientFixedNewstyle = uint32(1 << 0)
    nbdFlagClientNoZeroes      = uint32(1 << 1)
    nbdKnownClientFlags        = nbdFlagClientFixedNewstyle | nbdFlagClientNoZeroes
)

type handshakeState struct {
    clientFlags       uint32
    noZeroes          bool
    structuredReplies bool
    extendedHeaders   bool
    exportName        string
    metaContexts      []nbdMetaContext
}

func parseClientFlags(flags uint32) (handshakeState, error) {
    if flags&^nbdKnownClientFlags != 0 {
        return handshakeState{}, fmt.Errorf("unknown client flags: 0x%x", flags&^nbdKnownClientFlags)
    }
    return handshakeState{
        clientFlags: flags,
        noZeroes:    flags&nbdFlagClientNoZeroes != 0,
    }, nil
}
```

Update `newstyleHandshake` to return `(handshakeState, error)` and make `sendExportData` write 10 bytes when `state.noZeroes` is true.

- [x] **Step 4: Run focused tests**

Run:

```bash
go test ./internal/nbd -run 'TestNBDHandshake|TestNBDHandshakeRejectsUnknownClientFlags|TestNBDExportNameHonorsNoZeroes' -count=1
```

Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add internal/nbd/handshake.go internal/nbd/modern_test.go internal/nbd/nbd.go internal/nbd/nbd_test.go
git commit -m "feat: harden nbd fixed newstyle handshake"
```

## Task 2: OPT_INFO, OPT_GO, and Size Constraints

**Files:**
- Modify: `internal/nbd/handshake.go`
- Modify: `internal/nbd/modern_test.go`
- Modify: `internal/nbd/nbd.go`

- [x] **Step 1: Write failing option and block-size tests**

Add tests:

```go
func TestNBDOptGoValidatesExportName(t *testing.T) {
    client, _ := setupRawNBDConn(t)
    completeClientFlags(t, client, nbdFlagClientFixedNewstyle)
    writeOptGo(t, client, "missing", []uint16{nbdInfoExport})
    rep := readOptionReply(t, client)
    require.Equal(t, nbdRepErrUnknown, rep.typ)
}

func TestNBDOptInfoBlockSize(t *testing.T) {
    client, _ := setupRawNBDConn(t)
    completeClientFlags(t, client, nbdFlagClientFixedNewstyle)
    writeOptInfo(t, client, "nbd-test", []uint16{nbdInfoBlockSize})
    info := readInfoReply(t, client, nbdInfoBlockSize)
    require.Equal(t, uint32(1), binary.BigEndian.Uint32(info[2:6]))
    require.Equal(t, uint32(4096), binary.BigEndian.Uint32(info[6:10]))
    require.Equal(t, uint32(nbdMaxPayloadSize), binary.BigEndian.Uint32(info[10:14]))
    ack := readOptionReply(t, client)
    require.Equal(t, nbdRepAck, ack.typ)
}
```

- [x] **Step 2: Run tests and confirm failure**

Run:

```bash
go test ./internal/nbd -run 'TestNBDOptGoValidatesExportName|TestNBDOptInfoBlockSize' -count=1
```

Expected: FAIL because `OPT_INFO`, export validation, and `NBD_INFO_BLOCK_SIZE` are missing.

- [x] **Step 3: Implement `OPT_INFO`/`OPT_GO` parsing**

Add constants:

```go
const (
    nbdOptInfo = uint32(6)

    nbdRepErrUnsup   = uint32(1 | (1 << 31))
    nbdRepErrUnknown = uint32(6 | (1 << 31))
    nbdRepErrInvalid = uint32(3 | (1 << 31))

    nbdInfoExport    = uint16(0)
    nbdInfoBlockSize = uint16(3)

    nbdMinBlockSize       = uint32(1)
    nbdPreferredBlockSize = uint32(volume.DefaultBlockSize)
    nbdMaxPayloadSize     = uint32(32 * 1024 * 1024)
)
```

Parse option payload as:

```text
u32 nameLen
name bytes
u16 infoCount
u16 info requests...
```

Return `NBD_REP_ERR_UNKNOWN` for non-matching export names. Return `NBD_INFO_EXPORT` for every successful `OPT_INFO`/`OPT_GO`, and return `NBD_INFO_BLOCK_SIZE` when requested.

- [x] **Step 4: Run focused tests**

Run:

```bash
go test ./internal/nbd -run 'TestNBDOpt|TestNBDHandshake|TestNBDWriteRead' -count=1
```

Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add internal/nbd/handshake.go internal/nbd/modern_test.go internal/nbd/nbd.go
git commit -m "feat: support nbd opt info and size constraints"
```

## Task 3: Request Parser and Mutation Queue

**Files:**
- Create: `internal/nbd/request.go`
- Modify: `internal/nbd/nbd.go`
- Modify: `internal/nbd/modern_test.go`

- [x] **Step 1: Write failing size and ordering tests**

Add tests:

```go
func TestNBDReadRejectsOversizeBeforeAllocation(t *testing.T) {
    _, conn := setupNBD(t)
    sendRawRequest(t, conn, nbdCmdRead, 0, uint64(nbdMaxPayloadSize)+1, nil, 0)
    reply := readSimpleReply(t, conn)
    require.Equal(t, uint32(nbdErrEINVAL), reply.errCode)
}

func TestNBDFlushOrdersWriteZeroesAndTrim(t *testing.T) {
    _, conn := setupNBD(t)
    sendWriteConn(t, conn, 0, bytes.Repeat([]byte{0xaa}, 4096))
    sendWriteZeroesConn(t, conn, 0, 4096, 0)
    sendTrimConn(t, conn, 4096, 4096)
    sendFlushConn(t, conn)
    got := sendReadConn(t, conn, 0, 4096)
    require.Equal(t, make([]byte, 4096), got)
}
```

- [x] **Step 2: Run tests and confirm failure**

Run:

```bash
go test ./internal/nbd -run 'TestNBDReadRejectsOversizeBeforeAllocation|TestNBDFlushOrdersWriteZeroesAndTrim' -count=1
```

Expected: FAIL because oversize validation and `WRITE_ZEROES` are absent.

- [x] **Step 3: Implement request parsing and pending mutation**

Create:

```go
type nbdRequest struct {
    flags  uint16
    typ    uint32
    handle [8]byte
    offset uint64
    length uint64
}

type pendingMutation struct {
    key uint64
    fn  func() error
}
```

Replace `pendingWrite` with `pendingMutation`. Validate `length <= nbdMaxPayloadSize` before calling `getBuf` or volume operations. Keep `TRIM` synchronous for now but treat it as a write-like command in tests.

- [x] **Step 4: Run focused tests**

Run:

```bash
go test ./internal/nbd -run 'TestNBD.*(Oversize|Flush|WriteRead|Trim|Zeroes)' -count=1
```

Expected: PASS for implemented tests.

- [x] **Step 5: Commit**

```bash
git add internal/nbd/request.go internal/nbd/nbd.go internal/nbd/modern_test.go
git commit -m "feat: bound nbd requests and generalize flush mutations"
```

## Task 4: WRITE_ZEROES

**Files:**
- Modify: `internal/nbd/nbd.go`
- Modify: `internal/nbd/modern_test.go`
- Modify: `internal/nbd/nbd_bench_test.go`

- [x] **Step 1: Write failing `WRITE_ZEROES` tests**

Add tests:

```go
func TestNBDWriteZeroesReadBack(t *testing.T) {
    _, conn := setupNBD(t)
    sendWriteConn(t, conn, 0, bytes.Repeat([]byte{0xcc}, 8192))
    sendWriteZeroesConn(t, conn, 4096, 4096, 0)
    got := sendReadConn(t, conn, 0, 8192)
    require.Equal(t, bytes.Repeat([]byte{0xcc}, 4096), got[:4096])
    require.Equal(t, make([]byte, 4096), got[4096:])
}

func TestNBDWriteZeroesRejectsFastZero(t *testing.T) {
    _, conn := setupNBD(t)
    sendWriteZeroesConn(t, conn, 0, 4096, nbdCmdFlagFastZero)
    reply := readSimpleReply(t, conn)
    require.NotEqual(t, uint32(0), reply.errCode)
}
```

- [x] **Step 2: Run tests and confirm failure**

Run:

```bash
go test ./internal/nbd -run 'TestNBDWriteZeroes' -count=1
```

Expected: FAIL.

- [x] **Step 3: Implement correctness-first zeroing**

Add `nbdCmdWriteZeroes = uint32(6)` and advertise `nbdFlagSendWriteZeroes`. For the first implementation, write zero chunks using pooled 4 KiB zero buffers through `WriteAtDeferred`. Reject `FAST_ZERO`; accept `NO_HOLE` as a hint with no semantic change.

- [x] **Step 4: Add and run benchmark**

Add `BenchmarkNBD_WriteZeroes4K` and run:

```bash
go test ./internal/nbd -run '^$' -bench 'BenchmarkNBD_(Write4K|WriteZeroes4K)' -benchmem -count=1
```

Expected: benchmark completes and allocation deltas are reviewed.

- [x] **Step 5: Commit**

```bash
git add internal/nbd/nbd.go internal/nbd/modern_test.go internal/nbd/nbd_bench_test.go
git commit -m "feat: support nbd write zeroes"
```

## Task 5: Structured Replies and Block Status

**Files:**
- Create: `internal/nbd/reply.go`
- Modify: `internal/nbd/handshake.go`
- Modify: `internal/nbd/nbd.go`
- Modify: `internal/nbd/modern_test.go`
- Modify: `internal/nbd/nbd_bench_test.go`

- [x] **Step 1: Write failing structured reply and metadata tests**

Add tests:

```go
func TestNBDStructuredReadReply(t *testing.T) {
    client, _ := setupStructuredNBD(t)
    sendWriteConn(t, client, 0, []byte("abcd"))
    sendReadRequest(t, client, 0, 4)
    chunk := readStructuredChunk(t, client)
    require.Equal(t, nbdReplyTypeOffsetData, chunk.typ)
    require.Equal(t, uint16(nbdReplyFlagDone), chunk.flags)
    require.Equal(t, []byte("abcd"), chunk.payload[8:])
}

func TestNBDBlockStatusConservativeAllocated(t *testing.T) {
    client, _ := setupBlockStatusNBD(t)
    sendBlockStatusRequest(t, client, 0, 4096)
    chunk := readStructuredChunk(t, client)
    require.Equal(t, nbdReplyTypeBlockStatus, chunk.typ)
    require.Equal(t, uint32(1), binary.BigEndian.Uint32(chunk.payload[0:4]))
    require.Equal(t, uint32(4096), binary.BigEndian.Uint32(chunk.payload[4:8]))
    require.Equal(t, uint32(0), binary.BigEndian.Uint32(chunk.payload[8:12]))
}
```

- [x] **Step 2: Run tests and confirm failure**

Run:

```bash
go test ./internal/nbd -run 'TestNBDStructured|TestNBDBlockStatus' -count=1
```

Expected: FAIL.

- [x] **Step 3: Implement structured replies and metadata contexts**

Add `NBD_OPT_STRUCTURED_REPLY`, `NBD_OPT_LIST_META_CONTEXT`, `NBD_OPT_SET_META_CONTEXT`. Assign context ID `1` to `base:allocation`. Send `NBD_REPLY_TYPE_OFFSET_DATA` for reads in structured mode and `NBD_REPLY_TYPE_BLOCK_STATUS` for conservative allocated extents.

- [x] **Step 4: Run focused tests and benchmark**

Run:

```bash
go test ./internal/nbd -run 'TestNBD(Structured|BlockStatus|WriteRead|ReadReturnsError)' -count=1
go test ./internal/nbd -run '^$' -bench 'BenchmarkNBD_(Read4K|StructuredRead4K|BlockStatus4K)' -benchmem -count=1
```

Expected: PASS and benchmark output reviewed.

- [x] **Step 5: Commit**

```bash
git add internal/nbd/reply.go internal/nbd/handshake.go internal/nbd/nbd.go internal/nbd/modern_test.go internal/nbd/nbd_bench_test.go
git commit -m "feat: support nbd structured replies and block status"
```

## Task 6: Extended Header Parser Disabled by Default

**Files:**
- Modify: `internal/nbd/request.go`
- Modify: `internal/nbd/reply.go`
- Modify: `internal/nbd/handshake.go`
- Modify: `internal/nbd/modern_test.go`

- [x] **Step 1: Write failing disabled-advertisement and parser tests**

Add tests:

```go
func TestNBDExtendedHeadersUnsupportedByDefault(t *testing.T) {
    client, _ := setupRawNBDConn(t)
    completeClientFlags(t, client, nbdFlagClientFixedNewstyle)
    writeEmptyOption(t, client, nbdOptExtendedHeaders)
    rep := readOptionReply(t, client)
    require.Equal(t, nbdRepErrUnsup, rep.typ)
}

func TestNBDExtendedRequestParserRejectsOversizeEffect(t *testing.T) {
    req := make([]byte, 32)
    binary.BigEndian.PutUint32(req[0:4], nbdExtendedRequestMagic)
    binary.BigEndian.PutUint16(req[6:8], uint16(nbdCmdWriteZeroes))
    binary.BigEndian.PutUint64(req[24:32], uint64(nbdMaxPayloadSize)+1)
    _, err := parseExtendedRequest(req)
    require.Error(t, err)
}
```

- [x] **Step 2: Run tests and confirm failure**

Run:

```bash
go test ./internal/nbd -run 'TestNBDExtended' -count=1
```

Expected: FAIL.

- [x] **Step 3: Implement parser but keep negotiation unsupported**

Add `nbdOptExtendedHeaders = 11`, `nbdExtendedRequestMagic = 0x21e41c71`, and parser validation. Keep `handleOptExtendedHeaders` returning `NBD_REP_ERR_UNSUP` until interop passes.

- [x] **Step 4: Run focused tests**

Run:

```bash
go test ./internal/nbd -run 'TestNBDExtended|TestNBDStructured|TestNBDOpt' -count=1
```

Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add internal/nbd/request.go internal/nbd/reply.go internal/nbd/handshake.go internal/nbd/modern_test.go
git commit -m "feat: add disabled nbd extended header parser"
```

## Task 7: Interop Target and Documentation

**Files:**
- Create: `tests/nbd_interop/nbd_interop_test.go`
- Modify: `Makefile`
- Modify: `README.md`

- [x] **Step 1: Write optional interop smoke**

Create a Go test that skips clearly when `qemu-io`, `qemu-nbd`, or `libnbd` tools are unavailable:

```go
func TestNBDInteropToolsAvailable(t *testing.T) {
    if _, err := exec.LookPath("qemu-io"); err != nil {
        t.Skip("qemu-io not found; install qemu to run NBD interop smoke")
    }
}
```

Add follow-up tests that start `grainfs serve --nbd-port`, then run qemu/libnbd commands for `OPT_GO`, read/write, zero, and status when the tool supports them.

- [x] **Step 2: Add Makefile target**

Add:

```make
.PHONY: test-nbd-interop
test-nbd-interop: build
	GRAINFS_BINARY=$(CURDIR)/bin/$(BINARY) go test -v -timeout 180s ./tests/nbd_interop/
```

- [x] **Step 3: Update README**

Document:

```markdown
make test-nbd-interop
```

and state that extended headers remain disabled until qemu/libnbd interop passes.

- [x] **Step 4: Run commands**

Run:

```bash
go test ./tests/nbd_interop -count=1
go test ./internal/nbd -count=1
```

Expected: interop test passes or skips with a clear tool-missing message; NBD unit tests pass.

- [x] **Step 5: Commit**

```bash
git add tests/nbd_interop/nbd_interop_test.go Makefile README.md
git commit -m "test: add nbd modern interop smoke"
```

## Task 8: Final Verification

**Files:**
- No new files unless verification finds a defect.

- [x] **Step 1: Run package tests**

```bash
go test ./internal/nbd ./internal/volume -count=1
```

Expected: PASS.

- [x] **Step 2: Run NBD benchmarks with allocation output**

```bash
go test ./internal/nbd -run '^$' -bench 'BenchmarkNBD_' -benchmem -count=1
```

Expected: benchmark output present; no unexplained steady-state allocation regression.

- [x] **Step 3: Run interop target**

```bash
make test-nbd-interop
```

Expected: PASS or clear SKIP for missing qemu/libnbd tooling.

- [x] **Step 4: Run build and diff checks**

```bash
go build -o bin/grainfs ./cmd/grainfs
git diff --check
```

Expected: PASS.

- [x] **Step 5: Commit final docs/test adjustments if any**

If any verification-driven docs or test tweaks were needed:

```bash
git add <changed-files>
git commit -m "chore: finalize nbd modern protocol support"
```
