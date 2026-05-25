# Raft RPC Persistent Stream + Heartbeat Coalescing (R+H)

> **Status**: **DELIVERED** (2026-05-02). Design v3 shipped as v0.0.16.0 (#135), v0.0.16.1 (#136 nil panic fix), v0.0.17.0 (#137 default ON). The `--quic-mux` flag was removed in v0.0.151.0 — mux is now unconditionally on; the per-peer ALPN fallback to the legacy path (cross-version peers) is retained. `--quic-mux-pool` / `--quic-mux-flush` remain as tuning knobs.
>
> **Validation (idle-N8, 5 nodes × 8 raft groups, shared raft-log BadgerDB):**
>
> | Metric | mux=off | mux=on | Δ |
> |--------|--------:|-------:|---:|
> | CPU samples (30s pprof) | 25.86s | 5.60s | **−78%** |
> | `syscall.rawsyscall` (recvmsg) | 35% | 2% | **−17x** |
> | `runtime.kevent` absolute time | 5.04s | 0.45s | **−91%** |
> | Wall-clock CPU% | 26.0 | 22.9 | −12% |
> | RSS | 278 MB | 484 MB | +74% (frame buffers) |
> | Goroutines | 241 | 329 | +37% (pool=4 readers, parked) |
>
> **Open follow-ups** (separate PRs):
> - **Meta-raft mux integration** — see the plan v1 section after the Codex 7-finding review. Preconditions: clean R+H load-N8 mux=on measurement and a separate or bundled fix for the possible R+H reply race (#5).
> - Clean load-N8 and load-N16 mux=on measurements after host contention and the e2e bucket race are fixed.
> - Pool size sweep (1/2/4/8) before resetting the default after measuring the +74% RSS impact.
>
> ---
>
> **Original draft below (2026-05-02 Draft v3, codex 3-pass review).** Kept for the design rationale + decision points (pool=4, ALPN versioning, snapshot bypass invariant). Implementation references match the merged code in `internal/raft/raft_conn.go`, `heartbeat_coalescer.go`, `group_transport_mux.go`.

> **Branch (when designed)**: `perf/quic-stream-reuse` (now merged)
> **Supersedes**: v2. Codex reported 3 P0, 4 P1, and 2 P2 findings; v3 resolves them.
> **Trigger**: load-N8 profile. QUIC syscalls consumed 70% of CPU because each message opened and closed a stream.

## Problem

Every RPC in `internal/transport/quic.go` uses `OpenStreamSync` + `Close` per message. With 8 per-group Raft groups, heartbeats every 50-200ms, and 4 peers, the cluster opens and closes a large number of streams. macOS kqueue wakes on each patch and CPU spikes.

## Goal

- Remove per-message stream open/close from Raft RPC paths for per-group and meta traffic.
- Coalesce heartbeats from N groups between the same peer pair into one wire frame while preserving the synchronous reply contract.
- Profile target: reduce combined `runtime.kevent` + `syscall.recvmsg` from 70% to 30% or less on load-N8 idle.
- C(50→200ms 4x) × R(remove stream open) × H(8x batching) = large CPU reduction.

## Scope (unchanged from v2)

| Target | Scope |
|----------|-------|
| `internal/raft/group_transport_quic.go` | Use RaftConn |
| `internal/raft/meta_transport_quic.go` | Use RaftConn (Codex P1 #5 correction) |
| `internal/raft/heartbeat_coalescer.go` (new) | Per-peer batching layer |
| `internal/raft/raft_conn.go` (new) | Persistent stream layer |
| `internal/transport/quic.go` Send/Call/CallFlatBuffer | **No change** |
| Snapshot send: `internal/raft/quic_rpc.go:124`, `meta_transport_quic.go:87` | **No change** (Codex P1 #6 correction) |

## Design

### 0. ALPN routing — listener-level (codex P0 #1)

The old `quic.go:145 handleInboundConnection` accepted every connection regardless of ALPN and decoded it as `BinaryCodec`, so mux connections were misdecoded.

**Fix**:
```go
// listener loop in quic.go (changed)
for {
    conn, err := listener.Accept(ctx)
    if err != nil { ... }
    state := conn.ConnectionState()
    alpn := state.TLS.NegotiatedProtocol
    switch {
    case strings.HasPrefix(alpn, "grainfs-mux-v1-"):
        go t.handleMuxInbound(conn)   // new path
    case strings.HasPrefix(alpn, "grainfs-"):
        go t.handleInboundConnection(conn)  // existing path
    default:
        conn.CloseWithError(codeBadALPN, "unknown ALPN")
    }
}
```

**Registry split**:
```go
type QUICTransport struct {
    legacyConns map[string]*quic.Conn   // existing (Send/Call/CallFlatBuffer)
    muxConns    map[string]*RaftConn    // new (raft RPC)
    // ...
}
```

Dialing side: when a caller requests a connection for raft RPC, call `t.getOrConnectMux(addr)` and dial with the `grainfs-mux-v1-<psk>` ALPN. The existing `getOrConnect` path keeps using `grainfs-<psk>`.

**Cross-version transitional**: the v3 binary advertises both ALPNs. It uses mux when the peer supports mux and falls back to legacy otherwise. ALPN negotiation happens during the TLS handshake.

### 1. RaftConn — per-peer stream pool (Codex P1 #4 correction)

```go
// internal/raft/raft_conn.go (new)
type RaftConn struct {
    conn     *quic.Conn
    streams  []*RaftStream    // pool, default size 4
    next     atomic.Uint64    // RR
    nextID   atomic.Uint64    // CONN-level corrID generator
    pending  sync.Map         // CONN-level: corrID -> chan callResult
    closed   atomic.Bool
}

type RaftStream struct {
    parent *RaftConn
    stream *quic.Stream
    sendMu sync.Mutex   // serialize writes on this stream
}
```

**Key point (Codex P1 #4)**: keep both `nextID` and `pending` on **RaftConn**. The caller allocates the corrID, selects one stream by round-robin for the write, and receives the response on the same stream because the server replies on the stream it read from. Readers are per stream, but they dispatch to conn-level pending state. Cross-stream corrID collisions cannot happen because the counter is atomic.

```go
func (rc *RaftConn) Call(ctx context.Context, req *Message) (*Message, error) {
    id := rc.nextID.Add(1)
    ch := make(chan callResult, 1)
    rc.pending.Store(id, ch)
    defer rc.pending.Delete(id)

    s := rc.streams[rc.next.Add(1)%uint64(len(rc.streams))]
    s.sendMu.Lock()
    err := writeFrame(s.stream, OpRequest, id, req)
    s.sendMu.Unlock()
    if err != nil { rc.markBroken(err); return nil, err }

    select {
    case res := <-ch:
        return res.msg, res.err
    case <-ctx.Done():
        return nil, ctx.Err()
    }
}
```

### 2. Wire format

```
+---------------------+----+----+--------+---------------+
| 4-byte BE length    | OP | EC | corrID | payload       |
+---------------------+----+----+--------+---------------+
```

- `length`: OP+EC+corrID+payload length. **Validate before alloc**, cap 16MB.
- `OP`:
  - `0x01` Request
  - `0x02` Response
  - `0x03` ResponseError
  - `0x04` Notify (one-way, e.g. ReadIndex ack)
  - `0x05` HeartbeatBatch (Codex P0 #2/#3 correction; see section 4)
  - `0x06` HeartbeatReplyBatch
  - `0x00`, `0x07-0xFF` reserved
- `EC` (1 byte): reserved (currently 0)
- `corrID` (8 byte BE): Notify=0; otherwise unique
- `payload`: raft envelope, the result of `encodeRPC` in group/meta transport. This is not the `codec.go` Message envelope. Codex P2 #8 correction.

**Frame max size**: 16MB. Heartbeat batches are small, and AppendEntries with entries gets a safe margin.

### 3. Per-stream reader

```go
func (s *RaftStream) reader() {
    rc := s.parent
    for {
        n, err := readUint32(s.stream)
        if err != nil { rc.markBroken(err); return }
        if n > MaxFrameSize { rc.markBroken(errFrameTooBig); return }
        buf := framePool.Get().([]byte)[:n]
        if _, err := io.ReadFull(s.stream, buf); err != nil {
            framePool.Put(buf[:0]); rc.markBroken(err); return
        }
        op, _, corrID, payload := unframe(buf)

        switch op {
        case OpRequest:
            // Bounded handler pool — codex
            select {
            case rc.handlerSem <- struct{}{}:
                go func() {
                    defer func() {
                        <-rc.handlerSem
                        framePool.Put(buf[:0])
                        if r := recover(); r != nil {
                            sendErrorFrame(s, corrID, fmt.Errorf("handler panic: %v", r))
                        }
                    }()
                    resp, err := dispatchRequest(payload)
                    if err != nil {
                        sendErrorFrame(s, corrID, err)
                    } else if resp != nil {
                        sendResponseFrame(s, corrID, resp)
                    } else {
                        // explicit empty response — codex (no implicit hang)
                        sendErrorFrame(s, corrID, errNoResponse)
                    }
                }()
            default:
                sendErrorFrame(s, corrID, errHandlerOverloaded)
                framePool.Put(buf[:0])
            }

        case OpResponse, OpResponseError:
            if v, ok := rc.pending.LoadAndDelete(corrID); ok {
                select {
                case v.(chan callResult) <- decodeResult(op, payload):
                default:  // caller already gone, drop
                }
            }
            framePool.Put(buf[:0])

        case OpNotify:
            dispatchNotify(payload)
            framePool.Put(buf[:0])

        case OpHeartbeatBatch:
            handleHeartbeatBatch(s, corrID, payload)  // section 4
            framePool.Put(buf[:0])

        case OpHeartbeatReplyBatch:
            handleHeartbeatReplyBatch(rc, corrID, payload)  // section 4
            framePool.Put(buf[:0])

        default:
            rc.markBroken(errUnknownOp); return
        }
    }
}
```

### 4. Heartbeat coalescing (Codex P0 #2/#3 correction)

#### 4a. Synchronous contract preserved

`GroupRaftSender.AppendEntries(peer, args) (*Reply, error)` remains unchanged. The coalescer batches on behalf of the caller, but **the caller still receives the result through the reply channel**.

```go
// internal/raft/heartbeat_coalescer.go
type HeartbeatCoalescer struct {
    rc       *RaftConn
    pending  map[string]*peerBatch  // peer addr -> batch
    mu       sync.Mutex
    flushDur time.Duration  // max coalescing window (e.g. 2ms)
}

type peerBatch struct {
    items   []batchItem
    timer   *time.Timer  // fires flushDur after first item
}

type batchItem struct {
    groupID string
    args    *AppendEntriesArgs
    replyCh chan replicationResult  // caller blocked here
}
```

**Submit path** (caller):
```go
func (hc *HeartbeatCoalescer) AppendEntries(peer, groupID, args) (*Reply, error) {
    // Heartbeat-only optimization: no entries → batch path.
    // Real AppendEntries with entries → bypass coalescer (rc.Call directly).
    if len(args.Entries) > 0 {
        return hc.callDirect(peer, groupID, args)
    }
    replyCh := make(chan replicationResult, 1)
    hc.enqueue(peer, batchItem{groupID, args, replyCh})

    select {
    case res := <-replyCh:
        return res.Reply, res.Err
    case <-ctx.Done():
        // remove from batch if not yet flushed; if already in-flight, wait
        hc.cancelOrAwait(peer, replyCh, ctx)
        return nil, ctx.Err()
    }
}
```

**Enqueue + flush**:
```go
func (hc *HeartbeatCoalescer) enqueue(peer string, item batchItem) {
    hc.mu.Lock()
    b, ok := hc.pending[peer]
    if !ok {
        b = &peerBatch{items: []batchItem{item}}
        b.timer = time.AfterFunc(hc.flushDur, func() { hc.flush(peer) })
        hc.pending[peer] = b
    } else {
        b.items = append(b.items, item)
    }
    hc.mu.Unlock()
}

func (hc *HeartbeatCoalescer) flush(peer string) {
    hc.mu.Lock()
    b, ok := hc.pending[peer]
    if !ok { hc.mu.Unlock(); return }
    delete(hc.pending, peer)
    hc.mu.Unlock()
    b.timer.Stop()

    // Build batch frame
    payload := encodeHeartbeatBatch(b.items)  // group_id + args[] for each
    corrID := hc.rc.nextID.Add(1)
    inFlight := make(map[string]chan replicationResult, len(b.items))
    for _, item := range b.items {
        inFlight[item.groupID] = item.replyCh
    }
    hc.inFlightBatches.Store(corrID, inFlight)  // batch-level pending

    // Send via mux conn
    s := hc.rc.pickStream()
    s.sendMu.Lock()
    err := writeFrame(s.stream, OpHeartbeatBatch, corrID, payload)
    s.sendMu.Unlock()
    if err != nil {
        // notify all waiters with error
        hc.inFlightBatches.Delete(corrID)
        for _, ch := range inFlight {
            select { case ch <- replicationResult{Err: err}: default: }
        }
    }
}
```

#### 4b. Receiver: dispatch each, batch reply

```go
func handleHeartbeatBatch(s *RaftStream, corrID uint64, payload []byte) {
    items := decodeHeartbeatBatch(payload)  // []{groupID, args}
    replies := make([]heartbeatReplyItem, len(items))

    // Dispatch each to local raft (concurrent, with bounded pool)
    var wg sync.WaitGroup
    for i, it := range items {
        wg.Add(1)
        go func(i int, it batchItem) {
            defer wg.Done()
            reply, err := dispatchToGroupRaft(it.groupID, it.args)
            replies[i] = heartbeatReplyItem{
                GroupID: it.groupID,
                Reply:   reply,
                Err:     err,
            }
        }(i, it)
    }
    wg.Wait()

    replyPayload := encodeHeartbeatReplyBatch(replies)
    s.sendMu.Lock()
    writeFrame(s.stream, OpHeartbeatReplyBatch, corrID, replyPayload)
    s.sendMu.Unlock()
}
```

#### 4c. Sender: dispatch reply batch back

```go
func handleHeartbeatReplyBatch(rc *RaftConn, corrID uint64, payload []byte) {
    v, ok := rc.coalescer.inFlightBatches.LoadAndDelete(corrID)
    if !ok { return }  // expired
    inFlight := v.(map[string]chan replicationResult)

    items := decodeHeartbeatReplyBatch(payload)
    for _, item := range items {
        ch, ok := inFlight[item.GroupID]
        if !ok { continue }
        select {
        case ch <- replicationResult{Reply: item.Reply, Err: item.Err}:
        default:  // caller gone
        }
    }
}
```

#### 4d. Latency analysis (Codex P1 #7 correction)

- **flush window**: 2ms (1% of the 200ms HeartbeatTimeout). It does not affect the next 200ms tick and still allows enough time to form a batch.
- **First request to flush**: 0-2ms. Flush happens after 2ms or immediately when the next request arrives.
- **Last request in batch**: close to 0ms because it arrived right before flush.
- Average added latency is 1ms, which is negligible against a 200ms heartbeat tick.
- AppendEntries with replicator entries bypasses the coalescer via `callDirect`, so it adds 0 latency.

### 5. Snapshot bypass (Codex P1 #6 correction)

Snapshot send:
- `internal/raft/quic_rpc.go:124` (per-group)
- `internal/raft/meta_transport_quic.go:87` (meta)

These two functions keep using the existing `t.Call` legacy transport. They do not use RaftConn. v3 code verifies this with a unit test:

```go
func TestSnapshotBypassesMux(t *testing.T) {
    // Static analysis: parse quic_rpc.go + meta_transport_quic.go,
    // verify InstallSnapshot calls do not import RaftConn / coalescer.
    // OR: e2e — install snapshot, verify mux frame counter unchanged.
}
```

### 6. Connection lifecycle

**Dial**:
```go
func (t *QUICTransport) getOrConnectMux(addr string) (*RaftConn, error) {
    t.mu.RLock()
    if rc, ok := t.muxConns[addr]; ok {
        t.mu.RUnlock()
        return rc, nil
    }
    t.mu.RUnlock()

    tlsConf := &tls.Config{
        InsecureSkipVerify: true,
        NextProtos: []string{"grainfs-mux-v1-" + t.pskHash},
    }
    conn, err := quic.DialAddr(ctx, addr, tlsConf, quicCfg)
    if err != nil { return nil, err }

    rc := newRaftConn(conn, t.muxPoolSize)
    if err := rc.openStreams(); err != nil { conn.Close(); return nil, err }
    rc.startReaders()

    t.mu.Lock()
    if existing, ok := t.muxConns[addr]; ok {
        t.mu.Unlock()
        rc.Close(); return existing, nil
    }
    t.muxConns[addr] = rc
    t.mu.Unlock()
    return rc, nil
}
```

**Accept**: `handleMuxInbound(conn)` — `AcceptStream` × poolSize → same reader.

**Failure**:
```go
func (rc *RaftConn) markBroken(err error) {
    if !rc.closed.CompareAndSwap(false, true) { return }
    for _, s := range rc.streams { s.stream.CancelRead(0) }
    rc.conn.CloseWithError(0, err.Error())
    // notify all pending
    rc.pending.Range(func(k, v any) bool {
        select { case v.(chan callResult) <- callResult{err: err}: default: }
        return true
    })
    rc.pending.Clear()
    // notify all in-flight batches
    rc.coalescer.failAll(err)
    // evict from registry
    rc.transport.evictMux(rc.peerAddr)
}
```

### 7. Feature flag wiring (Codex P2 #9 correction)

```go
// cmd/grainfs/serve.go
serveCmd.Flags().Bool("quic-mux", false, "use multiplexed raft RPC stream + heartbeat coalescing (perf experiment, default off until v0.0.14)")
serveCmd.Flags().Int("quic-mux-pool", 4, "stream pool size per peer when --quic-mux=true")
serveCmd.Flags().Duration("quic-mux-flush", 2*time.Millisecond, "heartbeat coalescing flush window")

// usage:
quicMux, _ := cmd.Flags().GetBool("quic-mux")
muxPoolSize, _ := cmd.Flags().GetInt("quic-mux-pool")
muxFlushDur, _ := cmd.Flags().GetDuration("quic-mux-flush")

quicTransport := transport.NewQUICTransport(transport.Config{
    PSK: clusterKey, MuxEnabled: quicMux, MuxPoolSize: muxPoolSize,
})

// Group raft transport
groupRaftMux := raft.NewGroupTransportMux(quicTransport, quicMux, muxFlushDur)

// Meta raft transport (Codex P1 #5; actual location)
metaRaftSender := raft.NewMetaTransportQUIC(quicTransport, quicMux, ...)
```

### 8. Failure modes (all Codex cases)

| Failure | Behavior |
|------|------|
| ALPN mismatch | TLS handshake fails with a clear error |
| ALPN routing missing | Listener rejects unknown ALPN |
| Frame > 16MB | Connection discarded |
| Handler panic | Recover and send OpResponseError |
| Handler nil response | Explicit errNoResponse OpResponseError |
| Handler pool saturated | Immediate OpResponseError for backpressure |
| Ctx cancel mid-Call | defer pending.Delete, late response drop |
| `ch <- nil` panic | `chan callResult{msg, err}` |
| Stream disconnect | Reader exits, all pending calls receive errors, connection discarded |
| Coalescer flush error | Every request in the inFlight batch receives an error |
| Heartbeat timeout (no reply) | Same handling as the existing raft replicator timeout |
| Snapshot install bypass missing | Enforced by unit test |
| Cross-version mismatch | ALPN fallback (legacy path) |

## Migration

### Rollout step 1: code (default off)
- Add `internal/raft/raft_conn.go` and `heartbeat_coalescer.go`.
- Add ALPN routing and the `muxConns` registry to `internal/transport/quic.go`.
- Add mux/legacy branches to `group_transport_quic.go` and `meta_transport_quic.go`.
- Advertise both ALPNs. Servers accept both, and clients try mux first.
- Keep `--quic-mux=false` as the default for the existing path.

### Rollout step 2: opt-in measurement
- `--quic-mux=true` e2e + perf
- **Gate**: idle-N8 CPU drops by **40% or more**, measured by combined `runtime.kevent` + `recvmsg`.
- Reconsider the design if it misses the gate.

### Rollout step 3: default on (after one release)
### Rollout step 4: remove legacy ALPN/code (after the deprecation period)

## Test Plan

### Unit (`internal/raft/raft_conn_test.go` + `heartbeat_coalescer_test.go`)

```
RaftConn:
1. TestRaftConn_PendingCorrelation — N concurrent Calls separated by conn-level corrID
2. TestRaftConn_StreamPoolRR — pool=4 distributes work evenly
3. TestRaftConn_StreamFailure — stream disconnect returns errors for every pending call
4. TestRaftConn_ConcurrentCloseCallRace — Close during Call returns an error without panic
5. TestRaftConn_ReaderPanicRecovery — reader survives handler panic
6. TestRaftConn_CtxCancel — ctx cancellation deletes pending state and drops late responses
7. TestRaftConn_FrameSizeMax — 16MB+1 byte discards the connection
8. TestRaftConn_HandlerPoolBounded — pool saturation returns OpResponseError
9. TestRaftConn_NoResponseHandler — handler nil → errNoResponse
10. TestRaftConn_RestartReconnect — new Call reconnects after a broken connection

ALPN/PSK:
11. TestQUICTransport_ALPNRouting — mux ALPN and legacy ALPN dispatch separately
12. TestQUICTransport_PSKMismatch — PSK mismatch fails the TLS handshake
13. TestQUICTransport_ALPNFallback — old client (legacy) ↔ new server (mux+legacy) uses legacy

Heartbeat coalescing:
14. TestCoalescer_BatchFlush — N group heartbeats become 1 batch frame and N dispatched replies
15. TestCoalescer_PerPeerSplit — different peers use different batches
16. TestCoalescer_FlushWindowTimer — 2ms timer is exact
17. TestCoalescer_EntriesBypass — len(Entries) > 0 → callDirect
18. TestCoalescer_BatchSendError — sendError delivers errors for every inFlight item
19. TestCoalescer_PartialReply — receiver sends only some replies
20. TestCoalescer_CallerCtxCancel — caller ctx cancellation prevents channel delivery
21. TestCoalescer_InflightAfterFlush — new enqueue after flush starts a new batch

Snapshot:
22. TestSnapshotBypassesMux — snapshot install in quic_rpc.go and meta_transport_quic.go does not pass through RaftConn

Integration:
23. TestRaftConn_AppendEntriesPipelining_E2E — pool=4 + replicator inflight=2 yields 8 concurrent in-flight calls
24. TestE2E_RaftMux_LongRunning — 5-minute load run with no leaks
25. TestE2E_RaftMux_CrossVersion — node A (mux) ↔ node B (legacy) works
```

### Bench (`raft_conn_bench_test.go` + `heartbeat_coalescer_bench_test.go`)
- BenchmarkRaftCall_Mux vs Legacy (latency p50/p99)
- BenchmarkCoalescer_Throughput (8 groups × 5 hb/s input → flush throughput)
- Stream pool sweep: pool ∈ {1, 2, 4, 8}

### Perf
Add a `--quic-mux=true` variant to the existing `cluster_perf_profile_test.go`.

## Effort Estimate (raised after Codex review)

- `raft_conn.go` + ALPN routing: ~350 lines (mux dial + accept + reader + pending)
- `heartbeat_coalescer.go`: ~300 lines (synchronous contract + batch send + reply dispatch)
- `quic.go` listener routing + `muxConns` registry: ~80 lines
- `group_transport_quic.go` rewire (mux/legacy branch): ~80 lines
- `meta_transport_quic.go` rewire: ~80 lines
- `serve.go` flag wiring: ~30 lines
- Frame codec (encode/decode): ~150 lines
- 25 unit tests: ~700 lines
- 5 benchmarks: ~150 lines
- Snapshot bypass enforcement test: ~100 lines

**Total ~2,020 lines, 8-12 days** for one engineer. Includes every P0/P1/P2 mitigation Codex flagged.

## NOT in scope (separate follow-up)

- General `Send`/`Call`/`CallFlatBuffer` changes, including S3 forwarding.
- Snapshot install mux integration. Large payloads keep using a dedicated stream.
- Stream priority
- 0-RTT
- Datagram

## Decision Points (locked)

| # | Item | Value |
|---|------|----|
| 1 | Stream pool default | **4** |
| 2 | ALPN | **`grainfs-mux-v1-<pskhash>`** |
| 3 | Coalescer flush window | **2ms** (1% of the 200ms heartbeat) |
| 4 | Snapshot bypass enforce | **unit test (static parse + e2e counter)** |
| 5 | nextID/pending location | **RaftConn (conn-level)** |
| 6 | Cross-version migration | **ALPN dual-advertise from rollout step 1** |
| 7 | Entries-bearing AE | **callDirect (coalescer bypass)** |

## Risks (v3)

| Risk | Mitigation |
|------|-----------|
| ALPN routing bug → silent misdecode | Explicit default reject + unit test 13 |
| Single stream HoL → raft pipelining blocked | pool=4 |
| PSK silent removal | Encode PSK hash in ALPN |
| Coalescer reply contract broken | Synchronous chan delivery, callDirect bypass for entries-bearing AE |
| Coalescer latency | 2ms flush window |
| Receiver dispatch delay | Bounded handler pool, concurrent dispatch per group |
| Snapshot path bypass broken | unit test 22 + e2e counter |
| FlatBuffer zero-copy | mux carries only raft envelope; CallFlatBuffer is not used |
| Cross-version mismatch | ALPN fallback keeps the legacy path |
| Pending leak on ctx cancel | `defer pending.Delete()` + select default |

## Follow-up: Meta-raft mux integration

> **Status**: planned (2026-05-02). Plan v1 after `/plan-eng-review` + codex outside voice (7 findings).
> **Pre-conditions**: clean R+H load-N8 mux=on measurement and Coalescer reply race (#5) fix.
> **Estimated gain**: meta traffic is roughly 4%. The main value is unifying the idle path and finding the possible R+H race.

### Decisions (locked)

| Decision | Choice | Why |
|----------|--------|-----|
| Wire identifier | magic groupID `__meta__` (no wire change) | Wire-compatible with v0.0.16/v0.0.17 nodes |
| Coalescer | meta + group share per-peer HeartbeatCoalescer | Single 2ms flush window and one fewer frame |
| Snapshot install | keep legacy `tr.Call(StreamMetaRaft)` | R+H invariant: 60s timeout, large payload |
| Receiver dispatch | Add the `__meta__` branch to both `handleMuxRequest` and `dispatchToLocalGroup` | Codex P0 #1: the heartbeat path only passes through `dispatchToLocalGroup` |
| metaNode registration | Automatic inside `NewMetaRaftQUICTransport`; separate from EnableMux | Codex P1 #3: guarantees wire startup order |

### Codex 7-finding resolution

| # | P | Issue | Fix |
|---|---|-------|-----|
| 1 | P0 | meta heartbeat does not pass through `dispatchToLocalGroup`, causing unknown group | Add a `__meta__` branch to `dispatchToLocalGroup` |
| 2 | P0 | Missing `__meta__` ID reservation allows user config to create a `__meta__` group | Add `validateGroupID(id)` at four boundaries: `meta_fsm.go` `applyPutShardGroup`, `group_lifecycle.go:48` `instantiateLocalGroup`, `group_transport_quic.go:49` `Register`, and the propose boundary. Reject `__meta__`, empty strings, and reserved `__` prefixes. |
| 3 | P1 | Startup order: `MetaRaftQUICTransport` at :399, `EnableMux` at :568 | Move `serve.go` so it creates `groupRaftMux` and calls `EnableMux` before calling `NewMetaRaftQUICTransport`. Consider folding the `EnableMux` API into a `NewGroupRaftQUICMux` option. |
| 4 | P1 | If the meta RPC 500ms timeout is exhausted inside mux call, legacy fallback receives an expired ctx | Add a separate `muxAttemptTimeout` to `muxCall`, recommended at 200ms. On expiry, use a fresh 500ms ctx for legacy fallback. |
| 5 | P1 | **HeartbeatCoalescer reply race**: `flush()` sends before `inFlight.Store`, so a fast receiver reply can arrive before corrID registration and get dropped | ✅ **DELIVERED in v0.0.18.1 (#140)**. Split `CoalescerSender` into `NextHeartbeatCorrID()` and `SendHeartbeatBatchWithCorrID(corrID, payload)`. `flush()` now allocates, stores, then sends. Send failure rolls back with `inFlight.Delete`. Regression test `TestCoalescer_FastReplyBeforeRegister` shipped. |
| 6 | P1 | mixed-version: a v0.0.17 receiver returns `opResponseError "unknown group"` for `__meta__`, but the sender only treats dial/send failure as fallback triggers | In `MetaRaftQUICTransport.muxCall`, fallback to legacy when the response error has the `unknown group __meta__` or `mux: unknown` prefix. Add an `ErrMuxUnknownGroup` sentinel. |
| 7 | P2 | direct entries-bearing mux dispatch needs meta to use `metaRPC*` constants | Give the `__meta__` branch in `handleMuxRequest` a switch that accepts both `rpcType*` and `metaRPC*`. **Correction**: simplify sender behavior by using the same `rpcTypeRequestVote/AppendEntries` constants as group on the `__meta__` path. The magic gid is the wire signal; `metaRPC*` remains legacy-only. |

### Wire (unchanged)

```
mux frame:  [4B len][1B OP][1B EC][8B corrID][payload]
payload:    [2B gidLen][gid bytes][raft envelope = encodeRPC(rpcType*, args)]
                                                       ^
                                                       gid="__meta__" uses the same encoding
```

### Sender flow (meta_transport_quic.go)

```
SendRequestVote / SendAppendEntries:
  if mux.MuxEnabled() && groupMux registered:
    muxCtx, cancel = context.WithTimeout(ctx, muxAttemptTimeout=200ms)
    payload = prefixGroupID("__meta__", encodeRPC(rpcTypeRequestVote, args))
    resp, err := muxCall(muxCtx, peer, payload)
    cancel()
    if err == nil: return decode(resp)
    if isUnknownGroupErr(err) || isDialErr(err) || ctx.Err() == nil:
      // fall through to legacy with FRESH ctx
    else:
      return nil, err  // genuine RPC error, propagate

  // legacy
  legacyCtx, cancel = context.WithTimeout(parentCtx, metaRaftRPCTimeout=500ms)
  defer cancel()
  return tr.Call(legacyCtx, peer, &Message{Type: StreamMetaRaft, Payload: encodeRPC(metaRPCRequestVote, args)})

SendInstallSnapshot:
  // unchanged — legacy only
  return tr.Call(ctx_60s, peer, &Message{Type: StreamMetaRaft, Payload: encodeRPC(metaRPCInstallSnapshot, args)})
```

### Receiver flow (group_transport_mux.go)

```
handleMuxRequest(payload):
  gid, body = extractGroupID(payload)
  rpcType, data = decodeRPC(body)
  if gid == metaGroupID:  // "__meta__"
    metaNode := m.metaNode.Load()
    if metaNode == nil: return errMetaNotRegistered (→ opResponseError)
    switch rpcType:
      case rpcTypeRequestVote: return encodeRPC(rpcTypeRequestVoteReply, metaNode.HandleRequestVote(...))
      case rpcTypeAppendEntries: return encodeRPC(rpcTypeAppendEntriesReply, metaNode.HandleAppendEntries(...))
  // existing path
  node := m.nodes.Load(gid); switch rpcType...

dispatchToLocalGroup(gid, args):
  if gid == metaGroupID:
    metaNode := m.metaNode.Load()
    if metaNode == nil: return nil, errMetaNotRegistered
    return metaNode.HandleAppendEntries(args), nil
  // existing path
  node := m.nodes.Load(gid); return node.HandleAppendEntries(args), nil
```

### `__meta__` reservation (codex P0 #2)

```go
// internal/raft/group_transport_quic.go (or new internal/raft/group_id.go)
const metaGroupID = "__meta__"

// validateGroupID rejects reserved/empty/system-prefixed names.
// Called from: meta_fsm.applyPutShardGroup, group_lifecycle.instantiateLocalGroup,
// GroupRaftQUICMux.Register, MetaRaft.ProposeShardGroup.
func validateGroupID(id string) error {
    if id == "" {
        return errors.New("groupID empty")
    }
    if id == metaGroupID {
        return fmt.Errorf("groupID %q is reserved for meta-raft", id)
    }
    if strings.HasPrefix(id, "__") {
        return fmt.Errorf("groupID %q uses reserved prefix '__'", id)
    }
    return nil
}
```

Validation applies at all of these points:
1. `internal/cluster/meta_fsm.go` `applyPutShardGroup` — reject during apply and log a warning. If existing data contains a `__meta__` group, emit a startup warning and skip it.
2. `internal/cluster/group_lifecycle.go:48` `instantiateLocalGroup` — validate entry.ID, fatal on failure.
3. `internal/raft/group_transport_quic.go:49` `Register` — defensive, panic + msg.
4. `internal/cluster/meta_raft.go` `ProposeShardGroup` — reject at the proposer boundary and return an error.

### Coalescer race fix (Codex P1 #5 — R+H area) — ✅ DELIVERED

**Status**: shipped as **PR #140 / v0.0.18.1** (precursor PR-A, 2026-05-02).

`CoalescerSender` interface split:
```go
type CoalescerSender interface {
    NextHeartbeatCorrID() uint64
    SendHeartbeatBatchWithCorrID(corrID uint64, payload []byte) error
    PeerAddr() string
}
```

`flush()` order: allocate corrID → build inflightBatch → `inFlight.Store` → `SendHeartbeatBatchWithCorrID`. On send failure, call `inFlight.Delete` and then `failBatch`. Regression test `TestCoalescer_FastReplyBeforeRegister` uses the fakeSender `onSend` hook to dispatch a synchronous reply during send. If the pre-fix order returns, the test fails with a 200ms timeout.

Meta-mux uses the same interface with one `*RaftConn` sender per peer.

### Test plan (expanded)

```
internal/raft/meta_transport_mux_test.go  (~120 LOC)
  TestMetaRaftMux_RequestVote_Mux
  TestMetaRaftMux_RequestVote_FallbackOnDial
  TestMetaRaftMux_RequestVote_FallbackOnUnknownGroup    # codex P1 #6
  TestMetaRaftMux_AppendEntries_Coalesced
  TestMetaRaftMux_AppendEntries_Direct                   # entries-bearing
  TestMetaRaftMux_AppendEntries_FallbackOnTimeout        # codex P1 #4 budget
  TestMetaRaftMux_SnapshotBypassesMux                    # regression (mandatory)

internal/raft/group_transport_mux_test.go (expanded ~50 LOC)
  TestGroupMux_DispatchToLocalGroup_Meta                 # codex P0 #1
  TestGroupMux_HandleMuxRequest_Meta
  TestGroupMux_MetaNotRegistered_ErrorFrame
  TestGroupMux_RegisterMetaNode_Idempotent
  TestGroupMux_RejectsReservedGroupID                    # codex P0 #2

internal/raft/heartbeat_coalescer_test.go
  TestCoalescer_FastReplyBeforeRegister                  # ✅ shipped in v0.0.18.1 (#140)

internal/raft/meta_transport_mux_test.go (add ~20 LOC)
  TestMetaRaftMux_FastReplyBeforeRegister                # reuse the PR-A pattern (onSend hook)

internal/cluster/meta_fsm_test.go (expanded ~20 LOC)
  TestMetaFSM_RejectsReservedGroupID                     # codex P0 #2

internal/cluster/group_lifecycle_test.go (expanded ~15 LOC)
  TestInstantiateLocalGroup_RejectsReservedID            # codex P0 #2

tests/e2e/cluster_perf_profile_test.go (expanded)
  GRAINFS_PERF_META_MUX=1 environment branch for the mux=on + meta-mux on integration scenario
```

**REGRESSION RULE**: `TestMetaRaftMux_SnapshotBypassesMux` mandatory. (`TestCoalescer_FastReplyBeforeRegister` shipped in PR-A.)

### Effort re-estimate (updated after PR-A merge)

| Area | LOC |
|------|-----|
| `meta_transport_quic.go` mux/legacy branch + muxCall | ~80 |
| `group_transport_mux.go` `__meta__` branch (handleMuxRequest + dispatchToLocalGroup) + RegisterMetaNode | ~30 |
| `validateGroupID` + 4 call sites | ~30 |
| `serve.go` startup order move | ~30 |
| Tests (meta_transport_mux_test + group_transport_mux expansion + meta_fsm + group_lifecycle + e2e) | ~220 |
| **Total** | **~390 LOC** |

R+H Coalescer race fix already shipped in PR-A (#140 / v0.0.18.1). This PR proceeds as one PR-B.

### Risks (meta-specific)

| Risk | Mitigation |
|------|-----------|
| Applying `__meta__` reservation to existing data causes startup failure | Warn and skip during apply; error only at the propose boundary |
| Mixed-version cluster: post-meta-mux sender ↔ v0.0.17 receiver | Treat unknown-group response as a fallback trigger (#6) |
| Mux conn disconnects near meta election | Keep the mux attempt budget shorter than the heartbeat window |
| Group raft regresses after Coalescer race fix | ✅ R+H unit/e2e tests and `FastReplyBeforeRegister` all passed in PR-A |
| `__meta__` reserved prefix conflicts with external operations tooling group-name policy | Update docs/operators/runbook.md and add a user-facing ROADMAP note |

### NOT in scope (excluded from the meta-mux PR)

- New ALPN. Keep the single mux ALPN (`grainfs-mux-v1-<psk>`).
- meta-raft heartbeat/election timing tuning.
- InstallSnapshot mux integration.
- Separate per-meta-RPC metrics. Use the same counters as group traffic and split by label only.

---

## GSTACK REVIEW REPORT

| Review | Trigger | Why | Runs | Status | Findings |
|--------|---------|-----|------|--------|----------|
| CEO Review | `/plan-ceo-review` | Scope & strategy | 0 | — | not run |
| Codex Review | `/codex review` | Independent 2nd opinion | 3 | issues_open | v1: 11 → v2: 9 → v3: 0 (R+H, all addressed); follow-up: **7 (2 P0 + 4 P1 + 1 P2)** for meta-mux integration — all reflected in §Follow-up |
| Eng Review | `/plan-eng-review` | Architecture & tests (required) | 2 | issues_open | R+H v1: 6 issues addressed in v3; meta-mux v1: 2 internal (A1/A2) + 7 codex = 9 total, all reflected in §Follow-up |
| Design Review | `/plan-design-review` | UI/UX gaps | 0 | — | n/a (backend) |

- **CODEX v1 (R+H)**: scope, bounded handler pool, pending cleanup, nil channel result, nil-handler error, frame-size cap, effort estimate → all addressed in v2/v3
- **CODEX v2 (R+H)**: ALPN routing, heartbeat sync contract, heartbeat reply, stream pool correlation, meta transport location, snapshot file location, coalescer tick latency, frame payload, flag wiring → all addressed in v3
- **CODEX v3 (meta-mux follow-up, 2026-05-02)**: missing dispatchToLocalGroup handling (P0 #1), missing `__meta__` reservation (P0 #2), startup order (P1 #3), fallback budget exhaustion (P1 #4), **HeartbeatCoalescer reply race in R+H** (P1 #5), mixed-version remote error fallback (P1 #6), separate meta RPC switch handling (P2 #7) → all reflected in §Follow-up plan v1
- **UNRESOLVED**: 0 (all R+H decisions resolved as DELIVERED; meta-mux 9-finding fully reflected, awaits implementation post R+H measurement)
- **VERDICT**: R+H DELIVERED (v0.0.17). Meta-mux plan v1 ready, pre-conditions: (a) R+H reply race fix PR, (b) load-N8 clean measurement.
