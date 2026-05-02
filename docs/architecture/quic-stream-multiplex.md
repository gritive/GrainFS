# Raft RPC Persistent Stream + Heartbeat Coalescing (R+H)

> **Status**: Draft v3 (2026-05-02)
> **Branch**: `perf/quic-stream-reuse`
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

## GSTACK REVIEW REPORT

| Review | Trigger | Why | Runs | Status | Findings |
|--------|---------|-----|------|--------|----------|
| CEO Review | `/plan-ceo-review` | Scope & strategy | 0 | — | not run |
| Codex Review | `/codex review` | Independent 2nd opinion | 2 | issues_open | v1: 11 issues, v2: 9 (3 P0 + 4 P1 + 2 P2). All addressed in v3. |
| Eng Review | `/plan-eng-review` | Architecture & tests (required) | 1 | issues_open | 6 issues v1, addressed in v2/v3 |
| Design Review | `/plan-design-review` | UI/UX gaps | 0 | — | n/a (backend) |

- **CODEX v1**: scope (raft only), bounded handler pool, pending cleanup, nil channel result, nil-handler error, frame-size cap, effort estimate → all addressed in v2/v3
- **CODEX v2**: ALPN routing, heartbeat sync contract, heartbeat reply, stream pool correlation, meta transport location, snapshot file location, coalescer tick latency, frame payload, flag wiring → all addressed in v3
- **UNRESOLVED**: 0 (all 7 decision points resolved)
- **VERDICT**: v3 ready for codex 3rd-pass verification, then implementation
