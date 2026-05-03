# NBD Modern Protocol Support Design

Date: 2026-05-03
Branch: `feat/nbd-modern-protocol`

## Goal

GrainFS should support modern NBD clients without regressing the existing Linux
`nbd-client` path. The work covers two layers:

1. Make the existing fixed newstyle negotiation spec-compliant and predictable.
2. Add modern NBD extensions: `WRITE_ZEROES`, structured replies,
   `BLOCK_STATUS` with `base:allocation`, and extended headers.

NBD does not have NFS-style protocol versions such as 4.0, 4.1, and 4.2. It has
handshake styles and negotiated options. GrainFS already sends a fixed newstyle
header, but the current server only implements a small subset of option haggling
and transmission commands.

## Current State

`internal/nbd/nbd.go` currently supports:

- Fixed newstyle server header.
- `NBD_OPT_EXPORT_NAME`, `NBD_OPT_GO`, `NBD_OPT_LIST`, and `NBD_OPT_ABORT`.
- Simple replies for `READ`, `WRITE`, `DISC`, `FLUSH`, and `TRIM`.
- Transmission flags for `HAS_FLAGS`, `SEND_FLUSH`, and `SEND_TRIM`.

The gaps are:

- Client flags are read but not validated.
- `NBD_FLAG_C_NO_ZEROES` is not honored.
- `OPT_GO` ignores its payload and does not validate the export name.
- `OPT_INFO` is not implemented.
- `STRUCTURED_REPLY`, `WRITE_ZEROES`, metadata contexts, `BLOCK_STATUS`, and
  `EXTENDED_HEADERS` are unsupported.
- Request parsing assumes the simple 28-byte request header.
- Reply writing is coupled to simple replies, which makes later structured and
  extended modes harder to add safely.

## Scope

This design treats full modern support as one feature branch with incremental,
reviewable commits:

1. Fixed newstyle compliance.
2. `WRITE_ZEROES`.
3. Structured replies and `BLOCK_STATUS` minimum compatibility.
4. Extended headers and interop tests/docs.

The first compatibility target is correctness with Linux `nbd-client`, qemu NBD
clients, and libnbd-style option fallback. Performance optimizations follow only
after negotiated behavior is correct.

## Architecture

Introduce a small per-connection negotiation state:

```go
type handshakeState struct {
    clientFlags       uint32
    noZeroes          bool
    structuredReplies bool
    extendedHeaders   bool
    exportName        string
    metaContexts      []nbdMetaContext
}
```

`newstyleHandshake` should return `handshakeState` instead of only an error.
`handleConn` should pass that state into the transmission loop. The request loop
then chooses the correct parser and reply writer based on the negotiated state.

Option handling should be split into focused helpers:

- `handleOptExportName`
- `handleOptGo`
- `handleOptInfo`
- `handleOptList`
- `handleOptStructuredReply`
- `handleOptListMetaContext`
- `handleOptSetMetaContext`
- `handleOptExtendedHeaders`
- `handleUnsupportedOpt`

Reply writing should be separated behind methods that make the selected wire
format explicit:

- `sendSimpleReply`
- `sendStructuredReadReply`
- `sendStructuredErrorReply`
- `sendBlockStatusReply`
- `sendExtendedReply`

The existing simple reply path remains the default unless the client negotiates
structured replies or extended headers.

## Fixed Newstyle Compliance

The server will keep advertising fixed newstyle. After reading client flags, it
will accept only known flags:

- `NBD_FLAG_C_FIXED_NEWSTYLE`
- `NBD_FLAG_C_NO_ZEROES`

Unknown client flags will terminate negotiation with a clear error. If
`NBD_FLAG_C_NO_ZEROES` is set, the server will omit the 124 trailing zero bytes
in the `NBD_OPT_EXPORT_NAME` reply.

`NBD_OPT_GO` and `NBD_OPT_INFO` will parse the client payload, validate the
requested export name against `Server.volName`, and return `NBD_REP_ERR_UNKNOWN`
for unknown exports. Successful `OPT_GO` returns `NBD_INFO_EXPORT` followed by
`NBD_REP_ACK` and enters transmission. Successful `OPT_INFO` returns requested
information and stays in option haggling.

Unsupported options will return `NBD_REP_ERR_UNSUP` and keep the connection in
option haggling when the NBD protocol allows fallback.

## Size Constraints

Modern NBD clients can discover server size constraints through
`NBD_INFO_BLOCK_SIZE` in `NBD_OPT_INFO` and `NBD_OPT_GO`. GrainFS should make
those limits explicit before advertising extensions that can describe larger
operations.

Initial limits:

- Minimum block size: `1`, preserving existing unaligned read/write behavior.
- Preferred block size: `4096`, matching `volume.DefaultBlockSize`,
  `nbdPoolBufSize`, and existing fio/nbd-client workloads.
- Maximum payload size: `32 MiB`, matching the NBD interoperability default.
- Maximum effect length for extended-header zero, trim, and block-status
  commands: `32 MiB` in the first implementation, even though the extended
  protocol can encode a 64-bit length.

The server will return `NBD_INFO_BLOCK_SIZE` when the client requests it through
`NBD_OPT_INFO` or `NBD_OPT_GO`. Requests exceeding the negotiated or default
maximum will fail with an NBD error instead of allocating based on client input.
This applies before read, write, write-zeroes, trim, and block-status handlers
allocate buffers or enter volume operations.

## WRITE_ZEROES

Add `NBD_CMD_WRITE_ZEROES` only after request flag parsing is explicit. The
server should advertise `NBD_FLAG_SEND_WRITE_ZEROES` when the command is
available.

Initial implementation prioritizes correctness:

- Zero the requested range without reading payload bytes.
- Reuse existing volume write/discard machinery where possible.
- If a true sparse zero-range primitive is not available, write pooled zero
  buffers in bounded chunks.
- Return an error if offset or length exceeds the volume size.

`NBD_CMD_FLAG_FAST_ZERO` should not be advertised or accepted as a guarantee
until GrainFS has a true fast zero path. If a client requests fast zero before
that path exists, the server returns an explicit unsupported error rather than
silently doing slow work.

### Write-Like Command Ordering

Before adding `WRITE_ZEROES`, replace the current per-connection `pendingWrite`
queue with a generic pending mutation queue. It should cover:

- `NBD_CMD_WRITE`
- `NBD_CMD_WRITE_ZEROES`
- `NBD_CMD_TRIM`

The queue stores deferred commit functions plus an ordering key. `FLUSH` drains
all pending mutations that were acknowledged before the flush request, preserving
ordering within the same affected block/range and allowing parallel commits for
independent ranges. This keeps NBD's persistence contract intact: every completed
write-like command before a `FLUSH` must be durable before the `FLUSH` reply is
sent.

`TRIM` currently calls `volume.Manager.Discard` synchronously. If the underlying
volume/backend path later gains deferred commits, it must enter this same queue
rather than bypassing flush ordering. Until then, `TRIM` remains synchronous but
is modeled as a write-like command in the request pipeline and tests.

## Structured Replies

`NBD_OPT_STRUCTURED_REPLY` enables structured replies for that connection.
Before negotiation, reads continue to use the existing simple reply format.

After negotiation:

- Successful reads return one `NBD_REPLY_TYPE_OFFSET_DATA` chunk followed by
  `NBD_REPLY_FLAG_DONE`.
- Read failures return structured error chunks.
- Partial read support can be added later, but the first implementation should
  produce a single chunk per successful read for simplicity.

The reply writer boundary is important: `handleRequest` should not hand-roll
wire headers. It should ask the negotiated reply writer to send the correct
simple or structured response.

## Metadata Contexts and BLOCK_STATUS

Support `base:allocation` through:

- `NBD_OPT_LIST_META_CONTEXT`
- `NBD_OPT_SET_META_CONTEXT`
- `NBD_CMD_BLOCK_STATUS`

The first implementation may return a conservative "allocated" extent for the
requested range. That is compatible and avoids inventing storage semantics that
the volume layer cannot yet answer quickly.

If the volume layer can expose block allocation state cheaply, add an internal
interface such as:

```go
type ExtentStatusProvider interface {
    BlockStatus(name string, off, length int64) ([]ExtentStatus, error)
}
```

This interface should be optional. When absent, the NBD layer falls back to the
conservative allocated response. That keeps protocol compatibility separate from
future sparse/read-amplification optimization.

## Extended Headers

`NBD_OPT_EXTENDED_HEADERS` changes request and reply header shapes. Add it after
structured replies and block status are stable.

Treat extended headers as experimental until interop proves otherwise. The code
may parse and test the mode in-process, but GrainFS must not advertise or
acknowledge `NBD_OPT_EXTENDED_HEADERS` in normal operation until qemu/libnbd
smoke tests pass against the implementation.

Extended mode should:

- Negotiate per connection.
- Parse extended request headers separately from simple request headers.
- Keep simple-header clients fully backward compatible.
- Enforce GrainFS maximum request size even if the extended header can describe
  larger counts.
- Return explicit NBD errors for unsupported large operations.

The implementation should not share brittle slice offsets between simple and
extended parsers. Use small parser functions with tests for exact byte layouts.

Shipping rule: if extended headers are not interop-verified by the time the
other modern NBD work is ready, leave the code path disabled and document it as
an experimental follow-up rather than advertising unstable support.

## Error Handling

Protocol errors during handshake should be explicit and should avoid panics or
partial reads that leave the connection in an ambiguous state.

Transmission phase errors should map to NBD reply errors:

- Unknown command: `EINVAL`.
- Unsupported negotiated feature or unsupported request flag: `ENOTSUP` when
  possible, otherwise `EINVAL`.
- Backend read/write failure: `EIO`.
- Invalid offset/length: `EINVAL`.

For structured reply mode, read errors should be sent as structured error
chunks. For simple reply mode, keep the existing simple error header behavior.

## Testing

Unit tests in `internal/nbd`:

- Client flag validation.
- `NO_ZEROES` export reply length.
- `OPT_GO` export validation.
- `OPT_INFO` stays in option haggling.
- Unsupported option fallback.
- `NBD_INFO_BLOCK_SIZE` payload layout in `OPT_INFO` and `OPT_GO`.
- Read and write requests above the max payload fail before buffer allocation.
- Extended-header zero, trim, and block-status effect lengths above the max fail
  before volume operations.
- Requests exactly at the max payload/effect boundary are accepted when otherwise
  valid.
- `WRITE_ZEROES` read-back.
- Structured reply negotiation and read chunk layout.
- Structured error chunk layout.
- `LIST_META_CONTEXT` and `SET_META_CONTEXT` for `base:allocation`.
- `BLOCK_STATUS` conservative allocated response.
- Extended header parser accepts valid requests and rejects malformed ones.

TCP tests:

- Existing minimal client path continues to pass.
- New minimal client variants cover `OPT_GO`, structured replies, block status,
  and extended headers.

Interop smoke tests where tools are available:

- Linux `nbd-client` still connects and runs the existing fio profile.
- `qemu-io` or libnbd can negotiate modern options and run read/write/zero/status
  smoke operations.
- Add a deterministic `make test-nbd-interop` target or equivalent script that
  detects qemu/libnbd tooling, prints a clear skip when unavailable, and runs
  the modern negotiation smoke when available.
- Do not advertise structured replies, block status, or extended headers in
  normal operation until the relevant interop smoke passes. In-process tests may
  cover disabled experimental code paths, but feature advertisement requires
  real-client verification.

Performance tests:

- Extend `internal/nbd/nbd_bench_test.go` with allocation-aware benchmarks for
  simple read/write, `WRITE_ZEROES`, structured read, `BLOCK_STATUS`, and the
  extended-header parser.
- Run the NBD benchmarks with `-benchmem` before and after implementation and
  treat new per-request allocations on steady-state transmission paths as a
  regression unless the allocation is explicitly justified.
- Keep real-client fio coverage through `make bench-nbd` and
  `make bench-nbd-cluster`; use pprof allocation profiles when throughput or
  allocation counters regress.
- Do not allocate request/reply headers on each steady-state request. Prefer
  stack arrays, pooled buffers, or caller-owned scratch buffers where the code
  remains readable.

Baseline already checked before writing this design:

```text
go test ./internal/nbd -count=1
```

## Rollout

Commit sequence:

1. Refactor handshake state and fixed newstyle compliance tests.
2. Add `WRITE_ZEROES`.
3. Add structured replies and metadata context negotiation.
4. Add `BLOCK_STATUS`.
5. Add extended headers.
6. Add docs and interop notes.

Each commit should keep existing simple `nbd-client` behavior working. If an
extension is partially present, the server must not advertise it until the
corresponding command/reply path is implemented and tested.

## Implementation Flow

```text
TCP conn
  |
  v
fixed newstyle header
  |
  v
client flags -> validate fixed/no-zeroes only
  |
  v
option haggling
  |-- EXPORT_NAME -------------------------> transmission(simple)
  |-- INFO/GO -> export + size info --------> haggling/transmission
  |-- STRUCTURED_REPLY -> state flag -------> haggling
  |-- LIST/SET_META_CONTEXT -> contexts ----> haggling
  |-- EXTENDED_HEADERS -> experimental gate -> haggling/transmission if enabled
  |
  v
request parser(simple or extended)
  |
  v
size/flag validation before allocation
  |
  v
handler READ/WRITE/ZEROES/TRIM/FLUSH/BLOCK_STATUS
  |
  v
reply writer(simple, structured, or extended)
```

## Failure Modes

| Flow | Realistic failure | Required handling | Required test |
|------|-------------------|-------------------|---------------|
| Client flags | Unknown flag from newer client | Hard disconnect during handshake | Unknown flag test |
| Export selection | `OPT_GO` asks for missing export | Option error, no transmission | Bad export test |
| Size constraints | Client sends huge length | Reject before allocation | Oversize read/write/effect tests |
| Write-like ordering | `WRITE_ZEROES` bypasses pending commits | Shared mutation queue drained by `FLUSH` | write->zero->flush ordering |
| Structured read | Backend read fails mid-request | Structured error chunk | structured error test |
| Block status | No sparse status provider exists | Conservative allocated extent | block-status fallback test |
| Extended headers | Client negotiates experimental mode | Disabled unless interop gate passes | disabled advertisement test |
| Real-client interop | qemu/libnbd option order differs from unit client | Dedicated interop target with clear skip/pass | `make test-nbd-interop` |

No failure mode may be silent. Every negotiated feature either returns a valid
NBD reply/error or remains unadvertised.

## Worktree Parallelization

The work is mostly sequential because the parser, negotiation state, and reply
writer contracts are shared. Parallelization is possible only after the fixed
newstyle foundation lands.

| Step | Modules touched | Depends on |
|------|-----------------|------------|
| Fixed newstyle + size constraints | `internal/nbd` | - |
| Pending mutation queue + `WRITE_ZEROES` | `internal/nbd`, `internal/volume` | Fixed newstyle |
| Structured replies + metadata contexts | `internal/nbd` | Fixed newstyle |
| `BLOCK_STATUS` provider fallback | `internal/nbd`, optional `internal/volume` | Metadata contexts |
| Extended headers experimental path | `internal/nbd` | Structured replies |
| Interop target/docs | `Makefile`, `tests`, `docs` | Feature-specific behavior |

Recommended lanes:

- Lane A: fixed newstyle + size constraints.
- Lane B: after Lane A, `WRITE_ZEROES` and mutation ordering.
- Lane C: after Lane A, structured replies + metadata contexts + block status.
- Lane D: after Lane C, extended headers experimental path.
- Lane E: interop target/docs can start after Lane A but must finish after the
  feature it verifies.

Conflict flag: lanes B, C, and D all touch `internal/nbd`; run them in parallel
only with clear file ownership, otherwise keep them sequential.

## Non-Goals

- TLS support through `NBD_OPT_STARTTLS`.
- Multi-conn consistency guarantees.
- Persistent reservations or SCSI-style semantics.
- A new block storage backend.
- Changing NFS behavior.

These are separate protocol and consistency projects. The only storage-layer
extension in scope is optional block allocation status for better
`base:allocation` answers.
