# Raft RPC Persistent Stream + Heartbeat Coalescing (R+H)

> **Status**: **DELIVERED** (2026-05-02). Design v3 shipped as v0.0.16.0 (#135), v0.0.16.1 (#136 nil panic fix), v0.0.17.0 (#137 default ON).
>
> **Validation (idle-N8, 5 nodes × 8 raft groups, shared raft-log BadgerDB):**
>
> | Metric | mux=off | mux=on | Δ |
> |--------|--------:|-------:|---:|
> | CPU samples (30s pprof) | 25.86s | 5.60s | **−78%** |
> | `syscall.rawsyscall` (recvmsg) | 35% | 2% | **−17x** |
> | `runtime.kevent` 절대 시간 | 5.04s | 0.45s | **−91%** |
> | Wall-clock CPU% | 26.0 | 22.9 | −12% |
> | RSS | 278 MB | 484 MB | +74% (frame buffers) |
> | Goroutines | 241 | 329 | +37% (pool=4 readers, parked) |
>
> **Open follow-ups** (별도 PR):
> - **Meta-raft mux 통합** — plan v1 (post codex 7-finding) 본 문서 §Follow-up: Meta-raft mux integration 참고. 사전 조건: R+H load-N8 mux=on 깨끗한 측정 + R+H 잠재 reply race(#5) 분리 PR 또는 동봉.
> - load-N8 / load-N16 mux=on 깨끗한 측정 (host contention + e2e bucket race 해결 후)
> - pool size sweep (1/2/4/8) — RSS +74% 영향 평가 후 default 재조정
>
> ---
>
> **Original draft below (2026-05-02 Draft v3, codex 3-pass review).** Kept for the design rationale + decision points (pool=4, ALPN versioning, snapshot bypass invariant). Implementation references match the merged code in `internal/raft/raft_conn.go`, `heartbeat_coalescer.go`, `group_transport_mux.go`.

> **Branch (when designed)**: `perf/quic-stream-reuse` (now merged)
> **Supersedes**: v2 (codex가 3 P0 + 4 P1 + 2 P2 지적, 모두 본 v3에서 해결)
> **Trigger**: load-N8 프로파일 — CPU의 70%가 QUIC syscall, 원인은 메시지마다 stream open/close.

## Problem

`internal/transport/quic.go`의 모든 RPC가 메시지당 `OpenStreamSync` + `Close`. Per-group raft 8 그룹 × heartbeat (50→200ms) × peer 4개 → 클러스터 전체 대량 STREAM open/close. macOS kqueue가 patch마다 wake → CPU 폭증.

## Goal

- Raft RPC (per-group + meta) 경로의 per-message stream open/close 제거.
- 같은 peer pair에서 N 그룹의 heartbeat을 1 wire frame으로 coalescing **— synchronous reply 계약 유지**.
- Profile 효과: `runtime.kevent` + `syscall.recvmsg` 합산 70% → ≤30% (load-N8 idle).
- C(50→200ms 4x) × R(stream open 제거) × H(8x batching) = CPU drasitic.

## Scope (v2 대비 변경 없음)

| 변경 대상 | Scope |
|----------|-------|
| `internal/raft/group_transport_quic.go` | RaftConn 사용 |
| `internal/raft/meta_transport_quic.go` | RaftConn 사용 (codex P1 #5 정정) |
| `internal/raft/heartbeat_coalescer.go` (신규) | per-peer batching layer |
| `internal/raft/raft_conn.go` (신규) | persistent stream layer |
| `internal/transport/quic.go` Send/Call/CallFlatBuffer | **변경 없음** |
| Snapshot send: `internal/raft/quic_rpc.go:124`, `meta_transport_quic.go:87` | **변경 없음** (codex P1 #6 정정) |

## Design

### 0. ALPN routing — listener-level (codex P0 #1)

기존 `quic.go:145 handleInboundConnection`은 ALPN 무관하게 모든 conn을 받아 `BinaryCodec` decode → mux conn은 misdecode 됨.

**해결**:
```go
// listener loop in quic.go (수정)
for {
    conn, err := listener.Accept(ctx)
    if err != nil { ... }
    state := conn.ConnectionState()
    alpn := state.TLS.NegotiatedProtocol
    switch {
    case strings.HasPrefix(alpn, "grainfs-mux-v1-"):
        go t.handleMuxInbound(conn)   // 신규 path
    case strings.HasPrefix(alpn, "grainfs-"):
        go t.handleInboundConnection(conn)  // 기존 path
    default:
        conn.CloseWithError(codeBadALPN, "unknown ALPN")
    }
}
```

**Registry 분리**:
```go
type QUICTransport struct {
    legacyConns map[string]*quic.Conn   // 기존 (Send/Call/CallFlatBuffer)
    muxConns    map[string]*RaftConn    // 신규 (raft RPC)
    // ...
}
```

다이얼링 측: caller가 raft RPC 용 conn 요청 시 `t.getOrConnectMux(addr)` 호출 → `grainfs-mux-v1-<psk>` ALPN으로 dial. 기존 `getOrConnect`는 `grainfs-<psk>` 그대로.

**Cross-version transitional**: v3 binary는 양쪽 ALPN 모두 advertise하고, peer가 mux 지원하면 mux 사용 / 아니면 legacy fallback. Negotiation은 ALPN 자체가 처리 (TLS handshake).

### 1. RaftConn — peer당 stream pool (codex P1 #4 정정)

```go
// internal/raft/raft_conn.go (신규)
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

**핵심 (codex P1 #4)**: `nextID`/`pending` 모두 **RaftConn에 둠**. Caller가 corrID 발급 → write 시 stream 1개 선택 (RR), 응답은 같은 stream에서 받음 (server는 받은 stream으로 reply). Reader는 stream별이지만 dispatch 대상은 conn-level pending. Cross-stream corrID 충돌 없음 (atomic counter).

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

- `length`: OP+EC+corrID+payload 길이. **Validate before alloc**, cap 16MB.
- `OP`:
  - `0x01` Request
  - `0x02` Response
  - `0x03` ResponseError
  - `0x04` Notify (one-way, e.g. ReadIndex ack)
  - `0x05` HeartbeatBatch (코드 P0 #2/#3 정정 — section 4)
  - `0x06` HeartbeatReplyBatch
  - `0x00`, `0x07-0xFF` reserved
- `EC` (1 byte): reserved (현재 0)
- `corrID` (8 byte BE): Notify=0, 그 외 unique
- `payload`: raft envelope (group/meta transport의 `encodeRPC` 결과 — codec.go의 Message envelope 아님; codex P2 #8 정정)

**Frame max size**: 16MB. heartbeat batch는 작음, AppendEntries with entries는 크게 잡아 안전 마진.

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
                        // 명시적 empty response — codex (no implicit hang)
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

### 4. Heartbeat coalescing (codex P0 #2/#3 정정)

#### 4a. Synchronous contract 유지

`GroupRaftSender.AppendEntries(peer, args) (*Reply, error)`는 그대로. Coalescer가 caller 대신 batching하지만 **caller는 여전히 reply chan 통해 받음**.

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

#### 4d. Latency 분석 (codex P1 #7 정정)

- **flush window**: 2ms (HeartbeatTimeout 200ms의 1%) — 다음 200ms tick에 영향 없음, batch 성립 시간 충분.
- **First request → flush**: 0~2ms (2ms 후 flush 또는 다음 request 도착 시 즉시).
- **Batch 안의 마지막 request**: 거의 0ms (방금 도착해서 flush 됨).
- 평균 지연 1ms 추가 — heartbeat tick 200ms 대비 무시 가능.
- Replicator entries-가있는 AppendEntries는 coalescer 우회 (`callDirect`) → 지연 0.

### 5. Snapshot bypass (codex P1 #6 정정)

Snapshot send:
- `internal/raft/quic_rpc.go:124` (per-group)
- `internal/raft/meta_transport_quic.go:87` (meta)

이 두 함수는 기존 `t.Call` (legacy transport) 그대로 사용. RaftConn 미사용. v3 코드에서 검증 unit test:

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

**Accept**: `handleMuxInbound(conn)` — `AcceptStream` × poolSize → 같은 reader.

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

### 7. Feature flag wiring (codex P2 #9 정정)

```go
// cmd/grainfs/serve.go
serveCmd.Flags().Bool("quic-mux", false, "use multiplexed raft RPC stream + heartbeat coalescing (perf experiment, default off until v0.0.14)")
serveCmd.Flags().Int("quic-mux-pool", 4, "stream pool size per peer when --quic-mux=true")
serveCmd.Flags().Duration("quic-mux-flush", 2*time.Millisecond, "heartbeat coalescing flush window")

// 사용:
quicMux, _ := cmd.Flags().GetBool("quic-mux")
muxPoolSize, _ := cmd.Flags().GetInt("quic-mux-pool")
muxFlushDur, _ := cmd.Flags().GetDuration("quic-mux-flush")

quicTransport := transport.NewQUICTransport(transport.Config{
    PSK: clusterKey, MuxEnabled: quicMux, MuxPoolSize: muxPoolSize,
})

// Group raft transport
groupRaftMux := raft.NewGroupTransportMux(quicTransport, quicMux, muxFlushDur)

// Meta raft transport (codex P1 #5 — 실제 location)
metaRaftSender := raft.NewMetaTransportQUIC(quicTransport, quicMux, ...)
```

### 8. Failure modes (codex 모든 case)

| 실패 | 동작 |
|------|------|
| ALPN mismatch | TLS handshake fail, 명확한 error |
| ALPN routing 누락 | listener에서 unknown ALPN reject |
| Frame > 16MB | conn 폐기 |
| Handler panic | recover, OpResponseError 전송 |
| Handler nil response | 명시적 errNoResponse OpResponseError |
| Handler pool 포화 | OpResponseError immediate (backpressure) |
| Ctx cancel mid-Call | defer pending.Delete, late response drop |
| `ch <- nil` panic | `chan callResult{msg, err}` |
| Stream 끊김 | reader exit, pending all err, conn 폐기 |
| Coalescer flush err | inFlight batch 모두 err deliver |
| Heartbeat timeout (no reply) | 기존 raft replicator timeout과 동일 처리 |
| Snapshot install bypass 누락 | unit test로 enforce |
| Cross-version mismatch | ALPN fallback (legacy path) |

## Migration

### Phase 1: 코드 (default off)
- `internal/raft/raft_conn.go`, `heartbeat_coalescer.go` 추가
- `internal/transport/quic.go`에 ALPN routing + `muxConns` registry
- `group_transport_quic.go` + `meta_transport_quic.go`에 mux/legacy 분기
- ALPN 양쪽 advertise (서버는 둘 다, 클라이언트는 mux 우선 dial)
- `--quic-mux=false` default → 기존 path

### Phase 2: opt-in 측정
- `--quic-mux=true` e2e + perf
- **Gate**: idle-N8 CPU **40% 이상 감소** (`runtime.kevent`+`recvmsg` 합산)
- 안 되면 design 재고

### Phase 3: default on (한 release 후)
### Phase 4: legacy ALPN/code 제거 (deprecation 기간 후)

## Test Plan

### Unit (`internal/raft/raft_conn_test.go` + `heartbeat_coalescer_test.go`)

```
RaftConn:
1. TestRaftConn_PendingCorrelation — N 동시 Call, conn-level corrID로 분리
2. TestRaftConn_StreamPoolRR — pool=4 균등 분배
3. TestRaftConn_StreamFailure — stream 끊김 시 모든 pending err
4. TestRaftConn_ConcurrentCloseCallRace — Call 중 Close, panic 없이 err
5. TestRaftConn_ReaderPanicRecovery — handler panic 후 reader 살아남음
6. TestRaftConn_CtxCancel — ctx 취소, pending Delete, 늦은 응답 drop
7. TestRaftConn_FrameSizeMax — 16MB+1 byte → conn 폐기
8. TestRaftConn_HandlerPoolBounded — pool 포화 시 OpResponseError
9. TestRaftConn_NoResponseHandler — handler nil → errNoResponse
10. TestRaftConn_RestartReconnect — broken 후 새 Call이 reconnect

ALPN/PSK:
11. TestQUICTransport_ALPNRouting — mux ALPN과 legacy ALPN 분리 dispatch
12. TestQUICTransport_PSKMismatch — PSK 다르면 TLS handshake fail
13. TestQUICTransport_ALPNFallback — old client (legacy) ↔ new server (mux+legacy) — legacy 사용

Heartbeat coalescing:
14. TestCoalescer_BatchFlush — N 그룹 hb → 1 batch frame 후 N replies dispatched
15. TestCoalescer_PerPeerSplit — 다른 peer는 다른 batch
16. TestCoalescer_FlushWindowTimer — 2ms timer 정확
17. TestCoalescer_EntriesBypass — len(Entries) > 0 → callDirect
18. TestCoalescer_BatchSendError — sendError 시 모든 inFlight err deliver
19. TestCoalescer_PartialReply — receiver가 일부만 reply 보낸 경우
20. TestCoalescer_CallerCtxCancel — caller ctx 취소 시 chan deliver 안 됨
21. TestCoalescer_InflightAfterFlush — flush 후 새 enqueue가 새 batch 시작

Snapshot:
22. TestSnapshotBypassesMux — quic_rpc.go + meta_transport_quic.go의 Snapshot install이 RaftConn 안 거치는지 검증

Integration:
23. TestRaftConn_AppendEntriesPipelining_E2E — pool=4 + replicator inflight=2 → 8 동시 in-flight 정상
24. TestE2E_RaftMux_LongRunning — 5분 부하, leak 없음
25. TestE2E_RaftMux_CrossVersion — node A (mux) ↔ node B (legacy) 정상
```

### Bench (`raft_conn_bench_test.go` + `heartbeat_coalescer_bench_test.go`)
- BenchmarkRaftCall_Mux vs Legacy (latency p50/p99)
- BenchmarkCoalescer_Throughput (8 그룹 × 5 hb/s 입력 → flush 처리량)
- Stream pool sweep: pool ∈ {1, 2, 4, 8}

### Perf
기존 `cluster_perf_profile_test.go` + `--quic-mux=true` 변형 추가.

## Effort Estimate (codex 권고대로 재상향)

- `raft_conn.go` + ALPN routing: ~350 lines (mux dial + accept + reader + pending)
- `heartbeat_coalescer.go`: ~300 lines (synchronous contract + batch send + reply dispatch)
- `quic.go` listener routing + `muxConns` registry: ~80 lines
- `group_transport_quic.go` rewire (mux/legacy 분기): ~80 lines
- `meta_transport_quic.go` rewire: ~80 lines
- `serve.go` flag wiring: ~30 lines
- Frame codec (encode/decode): ~150 lines
- 단위 테스트 25개: ~700 lines
- 벤치 5개: ~150 lines
- Snapshot bypass enforcement test: ~100 lines

**총 ~2,020 lines, 8-12 일** (1인). codex가 지적한 모든 P0/P1/P2 mitigation 포함.

## NOT in scope (별도 follow-up)

- 일반 `Send`/`Call`/`CallFlatBuffer` 변경 (S3 forwarding 등)
- Snapshot install mux 통합 (large payload는 dedicated stream 유지)
- Stream priority
- 0-RTT
- Datagram

## Decision Points (확정)

| # | 항목 | 값 |
|---|------|----|
| 1 | Stream pool default | **4** |
| 2 | ALPN | **`grainfs-mux-v1-<pskhash>`** |
| 3 | Coalescer flush window | **2ms** (200ms heartbeat의 1%) |
| 4 | Snapshot bypass enforce | **unit test (static parse + e2e counter)** |
| 5 | nextID/pending 위치 | **RaftConn (conn-level)** |
| 6 | Cross-version migration | **ALPN dual-advertise from Phase 1** |
| 7 | Entries-bearing AE | **callDirect (coalescer 우회)** |

## Risks (v3)

| Risk | Mitigation |
|------|-----------|
| ALPN routing bug → silent misdecode | 명시적 default reject + unit test 13 |
| Single stream HoL → raft pipelining 죽음 | pool=4 |
| PSK silent removal | ALPN에 PSK hash 인코딩 |
| Coalescer reply contract 깸 | Synchronous chan deliver, callDirect bypass for entries-AE |
| Coalescer latency | 2ms flush window |
| Receiver dispatch 지연 | bounded handler pool, 그룹별 동시 dispatch |
| Snapshot path 우회 깨짐 | unit test 22 + e2e counter |
| FlatBuffer zero-copy | mux는 raft envelope만 담음 (CallFlatBuffer 미사용) |
| Cross-version mismatch | ALPN fallback (legacy path 유지) |
| Pending leak on ctx cancel | `defer pending.Delete()` + select default |

## Follow-up: Meta-raft mux integration

> **Status**: planned (2026-05-02). Plan v1 after `/plan-eng-review` + codex outside voice (7 findings).
> **Pre-conditions**: R+H load-N8 mux=on clean measurement 완료 + Coalescer reply race(#5) 수정.
> **Estimated gain**: meta 트래픽 ~4% — modest, 주 가치는 idle path 통일 + R+H 잠재 race 발견.

### Decisions (locked)

| Decision | Choice | Why |
|----------|--------|-----|
| Wire identifier | magic groupID `__meta__` (no wire change) | Wire-compat, 16/17 노드와 함께 굴러감 |
| Coalescer | meta + group share per-peer HeartbeatCoalescer | 단일 flush window 2ms, frame 1개 추가 절감 |
| Snapshot install | legacy `tr.Call(StreamMetaRaft)` 그대로 | R+H invariant — 60s timeout, large payload |
| Receiver dispatch | `__meta__` 분기를 `handleMuxRequest` + `dispatchToLocalGroup` 양쪽에 추가 | codex P0 #1 — heartbeat 경로는 `dispatchToLocalGroup`만 거침 |
| metaNode 등록 | `NewMetaRaftQUICTransport` 안에서 자동 + EnableMux 분리 | codex P1 #3 — wire 시작 순서 보장 |

### Codex 7-finding resolution

| # | P | Issue | Fix |
|---|---|-------|-----|
| 1 | P0 | meta heartbeat이 `dispatchToLocalGroup` 미경유 → unknown group | `dispatchToLocalGroup`에 `__meta__` 분기 추가 |
| 2 | P0 | `__meta__` ID reservation 누락 (user config로 `__meta__` group 가능) | `meta_fsm.go` `applyPutShardGroup`, `group_lifecycle.go:48` `instantiateLocalGroup`, `group_transport_quic.go:49` `Register`, propose 경계 4곳에 `validateGroupID(id)` 추가 — `__meta__`/빈 문자열/예약 prefix `__` 거부 |
| 3 | P1 | startup 순서 — `MetaRaftQUICTransport` :399, `EnableMux` :568 | `serve.go` 재배치: `groupRaftMux` 생성 + `EnableMux` 호출을 `NewMetaRaftQUICTransport` 호출 전으로 이동. `EnableMux` API를 `NewGroupRaftQUICMux` 옵션으로 흡수 검토 |
| 4 | P1 | meta RPC 500ms timeout이 mux call에서 다 소진되면 legacy fallback도 만료된 ctx | `muxCall`에 별도 `muxAttemptTimeout` (200ms 추천) — 만료 시 fresh ctx 500ms로 legacy fallback |
| 5 | P1 | **HeartbeatCoalescer reply race** — `flush()`가 `inFlight.Store` 전에 send → 빠른 receiver 응답이 corrID 미등록 상태에서 도착 → 드롭 | ✅ **DELIVERED in v0.0.18.1 (#140)**. `CoalescerSender` 인터페이스를 `NextHeartbeatCorrID()` + `SendHeartbeatBatchWithCorrID(corrID, payload)`로 split. `flush()`는 alloc → Store → Send 순. send 실패 시 `inFlight.Delete` rollback. 회귀 테스트 `TestCoalescer_FastReplyBeforeRegister` shipped. |
| 6 | P1 | mixed-version: v0.0.17 receiver가 `__meta__` 받으면 `opResponseError "unknown group"` 반환 → sender는 dial/send 실패만 fallback 인식 | `MetaRaftQUICTransport.muxCall`에서 응답 에러 메시지가 `unknown group __meta__`/`mux: unknown` prefix일 때도 legacy fallback. error sentinel `ErrMuxUnknownGroup` 추가 |
| 7 | P2 | direct(entries-bearing) mux dispatch 시 meta는 `metaRPC*` 상수 사용 필요 | `handleMuxRequest`의 `__meta__` 분기는 `rpcType*`/`metaRPC*` 둘 다 받도록 별도 switch. **수정**: 단순화 위해 sender 측에서 `__meta__` 경로는 group과 동일하게 `rpcTypeRequestVote/AppendEntries` 상수 사용 (wire에는 magic gid가 분기 신호) — `metaRPC*`는 legacy path 전용 |

### Wire (변경 없음)

```
mux frame:  [4B len][1B OP][1B EC][8B corrID][payload]
payload:    [2B gidLen][gid bytes][raft envelope = encodeRPC(rpcType*, args)]
                                                       ^
                                                       gid="__meta__" 도 동일 인코딩
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

Validation 적용 지점 (전부):
1. `internal/cluster/meta_fsm.go` `applyPutShardGroup` — apply 시 거부, log 경고. 기존 데이터에 `__meta__` group 있으면 startup warning + skip.
2. `internal/cluster/group_lifecycle.go:48` `instantiateLocalGroup` — entry.ID 검증, fatal.
3. `internal/raft/group_transport_quic.go:49` `Register` — defensive, panic + msg.
4. `internal/cluster/meta_raft.go` `ProposeShardGroup` — proposer 측 거부, error 반환.

### Coalescer race fix (codex P1 #5 — R+H 영역) — ✅ DELIVERED

**Status**: shipped as **PR #140 / v0.0.18.1** (precursor PR-A, 2026-05-02).

`CoalescerSender` 인터페이스 split:
```go
type CoalescerSender interface {
    NextHeartbeatCorrID() uint64
    SendHeartbeatBatchWithCorrID(corrID uint64, payload []byte) error
    PeerAddr() string
}
```

`flush()` 순서: alloc corrID → build inflightBatch → `inFlight.Store` → `SendHeartbeatBatchWithCorrID`. Send 실패 시 `inFlight.Delete` 후 `failBatch`. 회귀 테스트 `TestCoalescer_FastReplyBeforeRegister`는 fakeSender의 `onSend` 훅으로 mid-send에서 동기 reply dispatch — pre-fix 순서 회귀 시 200ms timeout으로 fail.

Meta-mux는 이 인터페이스를 그대로 사용 (sender = `*RaftConn`, peer당 1개).

### Test plan (확장)

```
internal/raft/meta_transport_mux_test.go  (~120 LOC)
  TestMetaRaftMux_RequestVote_Mux
  TestMetaRaftMux_RequestVote_FallbackOnDial
  TestMetaRaftMux_RequestVote_FallbackOnUnknownGroup    # codex P1 #6
  TestMetaRaftMux_AppendEntries_Coalesced
  TestMetaRaftMux_AppendEntries_Direct                   # entries-bearing
  TestMetaRaftMux_AppendEntries_FallbackOnTimeout        # codex P1 #4 budget
  TestMetaRaftMux_SnapshotBypassesMux                    # regression (mandatory)

internal/raft/group_transport_mux_test.go (확장 ~50 LOC)
  TestGroupMux_DispatchToLocalGroup_Meta                 # codex P0 #1
  TestGroupMux_HandleMuxRequest_Meta
  TestGroupMux_MetaNotRegistered_ErrorFrame
  TestGroupMux_RegisterMetaNode_Idempotent
  TestGroupMux_RejectsReservedGroupID                    # codex P0 #2

internal/raft/heartbeat_coalescer_test.go
  TestCoalescer_FastReplyBeforeRegister                  # ✅ shipped in v0.0.18.1 (#140)

internal/raft/meta_transport_mux_test.go (추가 ~20 LOC)
  TestMetaRaftMux_FastReplyBeforeRegister                # PR-A 패턴 재활용 (onSend hook)

internal/cluster/meta_fsm_test.go (확장 ~20 LOC)
  TestMetaFSM_RejectsReservedGroupID                     # codex P0 #2

internal/cluster/group_lifecycle_test.go (확장 ~15 LOC)
  TestInstantiateLocalGroup_RejectsReservedID            # codex P0 #2

tests/e2e/cluster_perf_profile_test.go (확장)
  GRAINFS_PERF_META_MUX=1 환경 분기 (mux=on + meta-mux on 통합 시나리오)
```

**REGRESSION RULE**: `TestMetaRaftMux_SnapshotBypassesMux` mandatory. (`TestCoalescer_FastReplyBeforeRegister` shipped in PR-A.)

### Effort re-estimate (PR-A 머지 후 갱신)

| 영역 | LOC |
|------|-----|
| `meta_transport_quic.go` mux/legacy 분기 + muxCall | ~80 |
| `group_transport_mux.go` `__meta__` 분기 (handleMuxRequest + dispatchToLocalGroup) + RegisterMetaNode | ~30 |
| `validateGroupID` + 4개 호출 지점 | ~30 |
| `serve.go` startup 순서 재배치 | ~30 |
| 테스트 (meta_transport_mux_test + group_transport_mux 확장 + meta_fsm + group_lifecycle + e2e) | ~220 |
| **총** | **~390 LOC** |

R+H Coalescer race fix는 PR-A (#140 / v0.0.18.1)로 이미 shipped — 본 PR은 단일 PR-B로 진행.

### Risks (meta-specific)

| Risk | Mitigation |
|------|-----------|
| `__meta__` reservation을 기존 데이터에 적용 시 startup 실패 | apply 단계에서 warning + skip, propose 단계에서만 error |
| Mixed-version cluster: post-meta-mux sender ↔ v0.0.17 receiver | unknown-group response를 fallback trigger로 인식 (#6) |
| Mux conn 끊김 + meta election 임박 | mux attempt budget 200ms < heartbeat 150ms 권장 |
| Coalescer race fix 후 group raft 회귀 | ✅ R+H unit/e2e 테스트 + `FastReplyBeforeRegister` 모두 PASS in PR-A |
| `__meta__` reserved prefix가 외부 운영 도구의 group 이름 정책과 충돌 | docs/RUNBOOK 갱신 + ROADMAP user-facing 메모 |

### NOT in scope (meta-mux PR에서 제외)

- 새로운 ALPN. 단일 mux ALPN(`grainfs-mux-v1-<psk>`) 그대로.
- meta-raft heartbeat/election timing 튜닝.
- InstallSnapshot mux 통합.
- Per-meta-RPC metric 분리 (group과 같은 카운터로 수집, label로 분리만).

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
- **CODEX v3 (meta-mux follow-up, 2026-05-02)**: dispatchToLocalGroup 누락 (P0 #1), `__meta__` reservation 부재 (P0 #2), startup 순서 (P1 #3), fallback budget 소진 (P1 #4), **HeartbeatCoalescer reply race in R+H** (P1 #5), mixed-version remote error fallback (P1 #6), meta RPC switch 분리 (P2 #7) → all reflected in §Follow-up plan v1
- **UNRESOLVED**: 0 (all R+H decisions resolved as DELIVERED; meta-mux 9-finding fully reflected, awaits implementation post R+H measurement)
- **VERDICT**: R+H DELIVERED (v0.0.17). Meta-mux plan v1 ready, pre-conditions: (a) R+H reply race fix PR, (b) load-N8 clean measurement.
