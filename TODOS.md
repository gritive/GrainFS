# TODOs

## Current Priority Lens: Operator Trust

Design principle: safe defaults, explicit escape hatches, observable operations.

Operators should see recovery, drain, rebalance, and upgrade state without
guessing from logs. Keep measured deferred work in this file. Promote an item to
active work only after reading the code/API surface and writing a specific
engineering plan.

Planning reference: operator trust roadmap note from 2026-05-15.

## Now

- [ ] **QUIC→TCP migration — S1 TCPTransport landed dormant; wiring-slice (S2/S3) must-fix items**
   - S1 added `internal/transport/{tcp_transport,tcp_call}.go`: a TLS-1.3-over-TCP, connection-per-RPC
     transport satisfying the transport-agnostic role interfaces (compile-time asserted in
     `cluster/raft/transport_iface.go`). It is **dormant** (not wired into boot; QUIC stays live).
     Code gate (3 reviewers + advisor) confirmed the framing invariant sound on all happy paths; the
     items below are robustness/faithfulness gaps that **cannot fire in dormant S1** (loopback, well-
     behaved peers) but MUST be addressed before S2/S3 wire TCPTransport into boot.
   - [ ] **[P1] Body-handler drain-on-all-paths, OR transport drain+deadline (B2 / correctness Finding 1+3).**
     The framing invariant requires a `HandleBody` handler to drain the body to `io.EOF` on **every**
     return path; the only prod body handler `HandleWriteBody` (`shard_service.go:802/805/809` +
     `readShardPayload` LimitReader) early-returns an error WITHOUT draining → unread client bytes →
     server `conn.Close()` emits RST → client gets `ECONNRESET` instead of the structured `StatusError`
     frame. Also a `CallWithBody`-vs-plain-`Handle` type mismatch truncates the same way. Fix at wire:
     either make every body handler drain on all paths, OR have `serveConn`'s hasBody branch
     `io.Copy(io.Discard, conn)` before close. **Coupling:** the transport-side drain is only safe with a
     server read deadline (a client that never `CloseWrite`s makes the discard block forever) — so this
     pairs with the deadline item below; do them together.
   - [ ] **[P2] Server-side read deadline + conn reaping (correctness Finding 2 / concurrency #1).**
     `serveConn` sets no deadline and `Decode` blocks on a bare `conn.Read`; `Close()` cancels `t.ctx`
     and closes only the listener, never the accepted conns → a stalled/dead peer leaks a goroutine+FD
     that survives shutdown (QUIC is backstopped by `MaxIdleTimeout`). Add a handshake/request-frame
     read deadline (cleared before long body transfers) and reap in-flight conns on `Close()`. This is
     spec §4c flow-control territory (deadlines, accept-rate, FD limits) — deferred by S1 design.
   - [ ] **[P2] Inbound admission control / `StatusOverloaded` (B3 / concurrency #3).** TCPTransport has
     no `TrafficLimiter`, so it never emits `StatusOverloaded` and `acceptLoop` spawns an unbounded
     `serveConn` goroutine per conn with no backpressure; a full `inbox` (cap 256) wedges gossip
     `serveConn`s until `Close()`. Spec §4b/S3 elastic pool is the intended throttle point.
   - [ ] **[P3] Confirm gossip anti-spoof parity at wire (unconfirmed).** A code-gate subagent reported
     QUIC and TCP both set `ReceivedMessage.From = conn.RemoteAddr()` (`host:ephemeralPort`) and gossip's
     `nodeIDMatchesFrom` (`gossip.go:382`) compares host-only — implying parity. This was NOT
     independently verified (rode in on a subagent report); confirm before wiring TCP into the gossip path.
   - [ ] **[P3] Pre-existing mux dial race-loser tears down the shared conn (surfaced by S2a review, NOT an S2a regression).**
     `GetOrConnectMux` dedupes two concurrent first-dials to the SAME `*quic.Conn`; in `muxConnFor` the race-loser then calls `rc.Close()` (group_transport_mux.go end-of-func), whose closeHook does `conn.CloseWithError` on that shared conn — tearing down the winner's live conn. Structurally identical in the old code (inline `rc.conn.CloseWithError`), so S2a preserved it (behavior-neutral). Suite-invisible. The S2b TCP driver reworks this dedup path; fix there (loser must not close a conn it doesn't solely own). **RESOLVED for TCP by S2b-2** (fresh outbound carrier per `GetOrConnectMux` → each owns only its own session's conns → loser's Close is self-contained; `TestTCPMux_RaceLoserOwnsOnlyOwnConns`). The QUIC path retains the shared-conn structure but is retired at S5/S6.
   - **S2a landed (carrier abstraction):** `RaftConn` is now carrier-agnostic over `io.ReadWriteCloser` (quic-go import removed from `raft_conn.go`); QUIC open/accept/init-frame moved to the mux driver. Proven behavior-neutral (5 characterization assertions identical pre/post + cluster suite) and carrier-agnostic (net.Pipe characterization incl. HBReply/Overload in-readLoop writes).
   - **S2b-1 LANDED (mux DRIVER carrier abstraction):** the per-group mux driver speaks `transport.MuxCarrier` (`OpenStream`/`AcceptStream`/`RemoteAddr`/`Close`) instead of `*quic.Conn`; `quicMuxCarrier` wraps the conn (comparable value → `muxConns` cache + `EvictMux` keep pointer-identity); `ClusterTransport`/`muxDriverTransport`/`MuxConnHandler` + `openMuxStreams`/`acceptMuxStreams` all carrier-agnostic; quic-go import dropped from `transport_iface.go` + `cluster_transport.go`. **Behavior-neutral** (live QUIC mux integration tests `internal/cluster` Mux/RaftV2Group/MetaRaft + raft char suite green under -race; discriminating audit confirmed the conn was used ONLY for open/accept-stream + CloseWithError + RemoteAddr — no datagrams/identity). Code gate (2 finders + advisor) CLEAN. **Test-proof note (advisor):** the inbound `handleInboundMuxConn(carrier)` path is EXECUTION-proven (multi-node raftv2_group_mux: each node accepts); the `OnBroken→EvictMux(carrier)` identity path is INSPECTION-proven only (no test forces a real break/re-dial — acceptable for a mechanical refactor, traced by 2 finders + the re-dial-then-stale-evict sequence). **S2b-1's non-QUIC carrier proof is deferred to S2b-2** (a net.Pipe MuxCarrier yielding N streams IS S2b-2's inbound-grouping problem in miniature — building it now would duplicate imminent work; do NOT re-litigate whether S2b-1 was under-tested).
   - **S2b-2 LANDED (dormant TCP mux driver):** `internal/transport/tcp_mux.go` implements the N-conn-per-peer `MuxCarrier`. `tcpOutboundMuxCarrier.OpenStream` dials a conn (mux-only `muxClientTLS`, `tcpMuxALPN`) stamped with a per-carrier `muxSessionID` (`[magic|sessionID]` frame, `io.ReadFull` exact — leaves the conn at the raft `opStreamInit` which RaftConn skips); `tcpInboundMuxCarrier` groups same-session conns (demux in `routeInboundMuxConn`, session-init read deadline-bounded) and invokes the mux handler ONCE per session. `serveConn` gained one post-handshake ALPN branch (mux→demux, else data-plane; `needClose` guard) + the server advertises `[tcpMuxALPN, tcpALPN]`. `GetOrConnectMux` returns a **fresh outbound carrier per call** (no addr-cache) → each owns only its own conns → the QUIC race-loser shared-conn teardown hazard (P3 below) **dissolves** — proven by execution (`TestTCPMux_RaceLoserOwnsOnlyOwnConns`: closing loser A leaves winner B's conn live). `Close` reaps both carrier sets. **Data-plane behavior-neutral** (existing TCP data-plane tests green + `TestTCPMux_DataPlaneConnStillNonMux`; NOT byte-identical — serveConn branch + advertised-ALPN set changed). **S2b-1's deferred non-QUIC carrier proof now DONE**: `TestRaftConnChar_TCP_*` runs the same RaftConn char assertions (echo + 50 concurrent corrID-muxed calls) over a real TCP carrier under `-race`. Code gate (2 finders + advisor): finder "split-brain via missing dedup" **REFUTED** (RaftConn is a transport carrier, leadership lives in the Node; both paths `lookupNode`→same local Node, idempotent by term/index; transient double-carrier self-heals on EOF — re-litigated a vetted design with the wrong failure model); finder-1 `incoming`-not-closed → AcceptStream post-pop `closed` re-check added (NOT `close(incoming)` — concurrent send would panic). Cluster QUIC mux integration tests green = neutrality backstop. Dormant: no production wiring (boot still `QUICMuxEnabled`).
   - **S4 LANDED (zero-CA join over TCP, dormant):** `internal/transport/tcp_join.go` adds `NewTCPJoinListener`/`DialJoinTCP` mirroring the QUIC join over crypto/tls (dedicated `JoinALPN` listener, permissive accept + peer-SPKI capture from `ConnectionState`, client-side SPKI pin, shared `ExportJoinBinding` + length-prefixed wire). **Commit 1** (behavior-neutral) abstracted `JoinHandler`/`DialJoin` off `*quic.Stream`→`io.ReadWriteCloser` (the consumer `HandleJoinStream` already took it; QUIC join byte-identical, proven by join+invite-join suites green; quic-go import dropped from join_listener_boot.go + 2 test files). **Half-close contract**: the consumer does `write→stream.Close()→read reply`, and `*quic.Stream.Close()` is a write-side half-close → `tcpJoinStream.Close()=tls.Conn.CloseWrite()` (NOT full close); separate closer = full teardown; leader `handleConn` drains the joiner's buffered close_notify before its full close (RST-robustness). Code gate (2 finders + advisor) CLEAN (finder "raw vs conn close on handshake-fail" REFUTED — matches `dial`/`dialMux` package convention: raw before handshake, conn after). Dormant: boot keeps the QUIC `JoinListener` until S5. **Security parity**: join security rests on invite-transcript validation + RFC5705 binding + client SPKI pin, NOT server-side cert checks — so permissive accept is correct on TCP too.
   - **S2c-1 LANDED (RaftConn lane separation):** `internal/raft/raft_conn.go` splits the per-peer streams into a `controlLane` (Call/Notify/heartbeat) and a `bulkLane` (`CallBulk`, entries-bearing AppendEntries up to MaxFrameSize) so a large AE cannot HoL-block a control-lane heartbeat/vote. Lane selection is **sender-local** (no wire change, no dialer/acceptor stream-index correspondence — the peer replies on the arrival stream and applies no lane policy). `RaftConnConfig.BulkLaneStreams==0` (QUIC default) makes control+bulk a single shared pool = the pre-S2c round-robin picker (**behavior-neutral on QUIC**, NOT byte-identical—adds methods+reclassifies 2 sites). Entries-bearing AE reclassified group(`group_transport_quic.go`)+meta(`meta_transport_quic.go`)→`CallBulk`; RequestVote+heartbeat stay control. `SetMuxBulkLaneStreams` threads a split into the OUTBOUND RaftConn (dormant: 0 on QUIC, set by S4). Inbound RaftConn stays single-lane (never picks). Deterministic HoL proof: `TestRaftConnLane_BulkStallDoesNotBlockControl` (stalled unread bulk stream, control `Call` still returns — no sleeps). QUIC cluster mux integration green = neutrality backstop. Code gate (2 finders + advisor): classification CLEAN (all 4 `rc.Call` sites verified, InstallSnapshot bypasses mux→legacy, heartbeat coalescer branch intact); fixed 4 stale "→RaftConn.Call" comments (advisor caught a 4th in raft_conn.go:28 the finders missed).
   - **S5-prereq LANDED (dormant acceptance-criteria discharge; test-only + 1 neutral seam).** Of the 3 criteria deferred across S2b-2/S2c-1/S4, items 1 & 2 are now fully discharged and item 3 is PARTIALLY advanced (see below). Each test was mutation-verified (RED on the targeted defect) — the `dev벤치≠패리티` / don't-checkbox discipline. The only production change is a behavior-neutral seam (`inviteJoinDialWith(dial joinDialer, ...)`; `inviteJoinDial` delegates with `transport.DialJoin` → production byte-identical, still QUIC).
     1. **[from S2c-1] Lane assignment under a REAL split — DISCHARGED.** `TestGroupRaftSender_EntriesAEUsesBulkLane_VoteUsesControlLane` drives the real `GroupRaftSender` through a fake mux carrier of tapped streams under `BulkLaneStreams=1`; observes at the SENDER (write-count delta) that entries-AE lands on the bulk-lane stream (zero on control) and RequestVote on a control-lane stream (zero on bulk). Mutation-verified: `CallBulk`→`Call` at group_transport_quic.go:239 routes entries-AE onto a control stream → RED (`deltas=[0 0 2 0]`).
     2. **[from S2b-2] Capability-exchange parity — DISCHARGED (point-in-time).** Audit: QUIC CE wire is `[]byte{ceVersion, ceFeatures}` with `ceFeatures==0` and `ceFeaturesSupportedMask==0` → NO feature negotiated; only the version byte is load-bearing. TCP carries version in the ALPN (`grainfs-tcp-mux-v1`); `TestTCPMux_VersionMismatchRejectedViaALPN` proves a bumped-ALPN mux dial fails the handshake specifically on `no application protocol` (+ positive control). **CAVEAT:** valid only while `ceFeaturesSupportedMask==0x00` — the first live CE feature bit on QUIC must be mirrored on the TCP ALPN/handshake or the TCP path silently drops it. Re-audit at that point.
   - [ ] **[from S4] Join RST-robustness under latency — STILL REQUIRED before TCP goes live (only partially advanced by S5-prereq).** `TestInviteJoinDialWith_RealConsumerOverDormantTCP` now drives the REAL consumer (`inviteJoinDialWith`) over the dormant TCP join transport against `NewTCPJoinListener` with the reply read forced after the leader's close — proving the consumer choreography + half-close + read-after-close decode over TCP. **BUT the leader drain / RST-avoidance is NOT discriminated on loopback** (mutation-confirmed: removing the drain at tcp_join.go handleConn keeps the test green — a tiny buffered reply makes clean-FIN and abortive-RST indistinguishable in-process). The empirical "no RST-truncation under real latency" proof needs netem / multi-node that forces the abortive-close path to surface as a read error.
   - **S5a LANDED (TCPTransport cluster identity/rotation surface; dormant, boot still QUIC).** `*TCPTransport` now satisfies the full `transport.ClusterTransport` interface (`var _ ClusterTransport = (*TCPTransport)(nil)`). S1 pinned a STATIC `IdentitySnapshot` in the TLS configs at construction; S5a converts it to QUIC's live model — `identity atomic.Pointer[IdentitySnapshot]` + an `identityComposer` whose swap stores into it — and rebuilds the 3 TLS configs FRESH per dial / per inbound handshake (`Listen` `GetConfigForClient`→`buildServerTLS`; `dial`/`dialMux`→`buildClientTLS`/`buildMuxClientTLS`). The 9 methods mirror QUIC: `SwapIdentity`/`UpdateRegistryAccept`/`SeedInitialPeerSPKIs`/`ApplyRotation`/`FlipPresent`/`SetDropped` delegate to the composer; `RecycleConns` drains pool+inbound conns+mux carriers; `ClosePeer` drains one peer's pool+carriers; `SetTrafficLimits` rebuilds the limiter. Tests are over-the-wire RED-able (accept-set/present-cert flips change a real Call's accept/reject; recycle actually closes conns). **Code gate (2 finders + advisor) caught a real security bug + its refinements:** RecycleConns/ClosePeer only drained IDLE conns, so a conn handshaked under a now-dropped identity could be reused — closed via a **pool identity-generation guard** on BOTH ends (checkin discards stale, checkout evicts stale) with the gen captured PRE-dial (closes the bump-during-handshake window). All RED-verified by neutering each branch. Neutrality: existing dormant TCP suite green (static snapshot = composer that never changes). **[P3] pre-existing `-race` flake** `TestTCPDeadline_SlowResponseReaderReaped` (timing under heavy `-race`) — attributed to base (flakes without S5a too), not this slice.
   - **S5b LANDED (boot transport-selection plumbing; dormant, default QUIC).** Boot can now construct the TCP cluster stack behind an internal/test-only `Config.useTCPTransport` flag (unexported; no operator surface, no CHANGELOG — production always builds QUIC, byte-identical). `bootQUICTransport`→`bootClusterTransport` branches ONLY the base-transport constructor (NewQUICTransport vs NewTCPTransport); all post-construction setup stays shared over the `ClusterTransport` interface (mux/meta/rpc constructors already take interfaces TCP satisfies since S5a). A small `joinListener` interface (Addr/SPKI/Close) lets `startJoinListener` branch QUIC vs `NewTCPJoinListener`. **Validation is in-process (no e2e tar pit):** (1) QUIC byte-identical (existing boot tests green); (2) `TestBootClusterTransport_TCP` + `TestStartJoinListener_TCPBranch` prove the branches fire; (3) `meta_raft_mux_tcp_integration_test.go` = 3-node meta-raft over the TCP **Call path** (meta discards the mux); (4) **`raftv2_group_mux_tcp_test.go` = 3-node group-raft over the S2b-2 TCP mux CARRIER** — its first multi-node exercise (passed; no integration bug). **Code gate (2 finders + advisor) caught a real coverage/labeling defect:** the meta spec did NOT exercise the mux carrier and overclaimed it; relabeled honestly + added the group-raft carrier test. Because `GroupRaftSender` falls back to `tr.Call` on mux failure, elect+replicate alone don't discriminate the carrier — the group test asserts `InboundMuxSessionCount > 0` (new accessor; the Call fallback never populates `muxInbound`), neuter-verified (skipping EnableMux keeps replication green via fallback but the carrier assertion fails `[0 0 0]`). **[P3] pre-existing mislabel:** the QUIC originals (`meta_raft_mux_integration_test.go` / `raftv2_group_mux_test.go`) carry the same "Sender/Receiver mux path" overclaim on the meta side — not fixed (pre-existing, out of S5b scope), captured here.
   - [ ] **[S5c — boot flip + bench gate, SIGN-OFF REQUIRED] remaining before TCP goes live:** flip the default → TCP (promote `useTCPTransport` to a real operator flag + CHANGELOG) + the §6 multi-node parity bench + the [from S4] Join RST-robustness netem/multi-node test above + the joiner-side `DialJoinTCP` wiring for invite-join over TCP + TCP mux-enable/bulk-lane wiring in the serveruntime boot path + a full serveruntime multi-node TCP cluster e2e (storage + S3 write/read across real nodes). TCP-live, **needs explicit sign-off + multi-node Linux infra.**
   - [ ] **[S2c-2 deferred] per-conn failure isolation.** Today one stream error → `markBroken` → full conn rebuild (correct, just wasteful). Partial isolation needs per-stream pending maps + closeHook restructuring + partial re-dial, all carrier-shaped on a carrier-agnostic RaftConn (antipattern). P2, speculative. **Trigger:** S5 multi-node TCP shows full-conn teardowns causing election churn. (Also reconcile the HB coalescer's conn-level `NextHeartbeatCorrID` with any per-lane split at that point.) **[also deferred] "대형 AE는 chunk"** (spec §4a) — entries-AE flows as one frame ≤16MB MaxFrameSize today; chunking a large AE into sub-frames is a separate refinement, revisit if a >16MB AE or finer HoL granularity is needed.
   - **Decision correction for the wiring slice:** the S1 plan's "skip `SetStreamHandler`" was WRONG —
     boot routes `StreamData` shard RPCs (ReadShard/ReadShardRange/Ping) through the catch-all
     (`SetStreamHandler` → `streamRouter.Dispatch`), invisible to the conformance assertions. S1 pulled
     the catch-all forward (implemented + tested). Remaining `ClusterTransport` gap = the mux-connection
     methods (`SetMuxConnHandler`/`GetOrConnectMux`/`EvictMux`), which are the S2 RaftConn restructure.
   - **S3a landed (data-plane chunked framing + minimal conn reuse):** `CallWithBody`/`CallRead` now use
     length-prefixed chunk framing (`tcp_chunk.go`, terminator-delimited) instead of CloseWrite, + a
     minimal per-peer conn pool (checkout/checkin, no cap) with drain-or-discard; `serveConn` is a
     persistent loop. Dormant; local bench is non-regression-only (parity is **Linux@S5**).
   - **S3b LANDED (data-plane resource bounds + elastic pool, `tcp_config.go`/`tcp_pool.go` + serveConn/serveOne/pool):**
     all S3b items above are now implemented (dormant, TCP-local, NO shared-codec change):
     server read/write deadlines (idle bound on the request Decode + body bound on handler read/drain +
     response-body egress Write); accepted-conn tracking + reaping on `Close`; transport-local desync
     detection via `Message.ID` stamp+echo+verify (the body-presence header flag was DROPPED — it would
     change the shared QUIC wire header (neutrality BLOCKER) and is redundant given detection + the type
     system); deterministic nil-resp→`StatusError`; elastic per-peer pool (cap+queue+growth+idle-age
     eviction, `cap=0` = S3a behavior); socket tuning (`TCP_NODELAY` + buffers); inbound admission
     (`TrafficLimiter` parity, Data/Bulk fail-fast to `StatusOverloaded`, Control/Meta block) + a
     concurrent-`serveConn` semaphore; retry-once on a stale reused conn that fails pre-body. Code gate
     (3 finders + advisor) caught a BLOCKER (handshake `SetDeadline` left an absolute WRITE deadline that
     killed any conn reused past `ServerIdleTimeout` — only the hasRead-body path cleared it; fixed +
     RED test) plus track-before-launch reap TOCTOU and a `closeAll` negative-total cap-bypass.
   - **S3b residual follow-ups (code-gate finders, [P3] unless noted):**
     - [ ] **Client-side body-read has no deadline (QUIC-parity gap).** `tcpReadCloser` hands the conn to
       the caller with the RPC-ctx deadline cleared (matches QUIC), so a server that stalls mid-body
       (without closing) pins a client goroutine + pooled slot until the caller's `Close`. The server
       side IS bounded (`ServerBodyTimeout`); the client read side is not. Revisit at S4/S5 wiring
       (client body-read idle deadline).
     - [ ] **Egress write-deadline has no QUIC counterpart.** `serveOne` hasRead bounds the response-body
       Write with `ServerBodyTimeout` (wall-clock, not idle-stall), so a *sustained-slow* (not stalled)
       reader of a large shard read can be dropped at 5m where QUIC's flow control would not. Generous,
       but a divergence to confirm acceptable at wire-in benchmark.
     - [ ] **Pool maps never delete empty keys.** `idle`/`total`/`waiters` retain a (tiny) entry per
       distinct peer addr forever; `waiters[addr]=q[1:]` advances without shrinking the backing array.
       Bounded by cluster peer count, negligible, but unbounded over process lifetime — delete empty keys
       if a churning-membership cluster ever makes it matter.
     - [ ] **Thundering-herd cancel = O(N) serial redistribution.** N waiters cancelling near-simultaneously
       produce a serial chain of `removeWaiter`→`checkin`/`dialFailed` hand-offs (each takes `mu` once).
       Correct (no leak), but a latency smell under a cancel storm. Revisit only if it shows in benchmarks.
     - [ ] **`cap==0` unlimited contract rests on "no waiter ever enqueued when cap==0".** `checkout`
       returns the dial slot before the enqueue path, so `dialFailed`/`discard` never hand a slot to a
       (nonexistent) waiter under `cap==0`. Defensive note: any future enqueue path must preserve this.

- [ ] **At-rest unification — R3 static-key retirement (last slice; cleanup, not migration)**
   - At-rest is **greenfield** — each format-changing slice bumps the on-disk format
     version and an older dir loud-fails on a newer binary (no in-place re-encrypt,
     no legacy ciphertext to support). Current format = **8**.
   - [ ] **[P2] PITR WAL torn-tail tolerance on encrypted replay (D5 follow-up, descoped from the
     DEK-PITR replay slice).** `ReplayEncrypted` is strict and errors on a final-segment torn frame
     (`TestWAL_EncryptedReplayRejectsTruncatedFrame` deliberately locks this; the plaintext path
     `break`s gracefully). The WAL does NOT self-heal the torn tail — `scanMaxSeq` only reads (no
     truncate) and the writer reopens `O_APPEND` (`wal.go:250`), so after a crash a torn frame
     persists and PITR restore **errors until that segment ages out of WAL retention**. Strictly
     better than before the DEK-PITR fix (encrypted PITR was 100% broken), but a real post-crash gap.
     Fix would tolerate a trailing `io.ErrUnexpectedEOF` on the FINAL segment in `wal.replay()` (index
     `i == len(files)-1`; `segmentFiles` sorts ascending) while keeping decrypt/auth + non-final torn
     fatal — this REVERSES the deliberate replay-strict contract, so update the existing test
     consciously. Keep parity with the plaintext path.
   - [ ] **Data-DEK rotation re-enable → full rekey lifecycle (epic; keep gated until prerequisites land).**
     User decision 2026-05-30: target the **full rekey lifecycle** (rotate → rewrap → prune,
     compromise recovery), executed slice-by-slice. Re-enable the `encryption.rotate-dek` trigger
     only after **all** ciphertext-bearing formats persist a non-zero `dek_gen` AND all data lanes
     carry per-entry/per-segment gen framing + roll-on-gen-change. Slice status:
     - **S1 packblob per-entry `dek_gen` framing — DONE** (this PR). Done via a self-describing
       per-entry flag bit (`flagGenFramed` + 4-byte gen between flags and data_len), **deliberately
       NOT the "format v8+" bump the spec anticipated** — packblob has no file/dir header to version,
       and rewrap needs mixed-gen entries in one append-only file, so per-entry self-describing
       framing is the permanent design. Read/Compact/ScanAll all gen-aware; gen is a key selector
       (not AAD-bound: only known post-`SealTo`; flipped gen fails closed via AEAD key-miss) +
       CRC-covered. Behavior-neutral at gen 0.
       **Compact = implicit partial rewrap (S7 prune-safety input):** post-rotation, `Compact`
       decrypts survivors then re-`Append`s them → re-seals under the **active** gen (no
       seal-under-specific-gen API). So Compact migrates entries off their pinned gen. **S7 prune
       MUST NOT assume Compact preserves an entry's gen** when deciding a gen is unreferenced.
     - S2 datawal `RollSegmentOnRotation` boundary method — SHIPPED (#676; method + unit
       tests, no production caller yet). Synchronous wiring (the gen-advance→roll window) is
       deferred to S5, where it is reachable end-to-end after `state.dataWAL` exists.
     - S3 legacy/logical WAL (`internal/storage/wal`) rotation boundary — SHIPPED (#678).
       Seal-first write path: seal once → roll when the sealed gen differs from the open
       segment's pinned gen, so the entry always lands in a header matching its gen. Removes
       the prior silent-drop (the deleted gen-mismatch assertion fed `writer()`'s log+continue
       after `lastSeq` advanced). Per-segment header gen (not per-record framing) — consistent
       with datawal. Behavior-neutral today (trigger gated); auto-activates at S5.
     - S4 close the EC mid-shard race — SHIPPED. seal-at-pinned-gen: chunk 0 seals at
       the active gen + records it in the header; chunks 1+ seal AT that pinned gen via a
       new gen-selecting seal (`encrypt.DEKKeeper.SealWithAADToAtGen` → `storage.DataEncryptor.SealAtGen`
       → `eccodec.ShardEncryptor.SealAtGen`). Closes the race in all THREE chunked encoders —
       `eccodec.EncodeEncryptedShard`, `EncryptedShardChunkedWriter` (cluster), and
       `storage.writeEncryptedObjectFile` (single-node GFOBJENC2; parity). A rotation racing an
       in-flight encode no longer fails the write; the removed `gen != pinnedGen` branch is
       unreachable by construction. Behavior-neutral when no rotation (gen == active → byte-identical
       to `Seal`); auto-activates at S5. **S7 dependency:** the pinned (possibly just-retired) gen must
       stay resident until a reference-safe Prune — S7 must treat an in-flight/Compact-held gen as
       referenced. Fail-closed if a gen is missing (`ErrDEKGenUnknown` → write error, no leak).
     - S5 enable `encryption.rotate-dek` — SHIPPED. The trigger now ACCEPTS "now" as a no-op at
       apply time (it runs inside the raft apply loop, where a blocking propose would deadlock); the
       post-commit dispatcher (`serveruntime/dek_post_commit.go`) does the actual leader-gated
       `ProposeDEKRotate` off the apply goroutine — mirroring the prune path (NOT a reload-hook, which
       would deadlock + double-propose). datawal `Append` now self-rolls across a gen advance via
       roll-then-retry BETWEEN locked attempts (`appendRecordOnce` + bounded retry calling the existing
       `RollSegmentOnRotation`), so concurrent appenders never duplicate a seq (the inline-park design
       the plan gate rejected). Legacy/logical WAL already self-heals (S3 seal-first); EC/object pin via
       S4. Old gens stay resident ⇒ pre-rotation data still decrypts; new writes use the new gen
       (mixed-gen is expected until S6 rewrap / S7 prune). Operator surface: `grainfs config set
       encryption.rotate-dek now`; confirm via `active_dek_generation` in the encryption status.
     - S6 rewrap scrubber: old-gen→new-gen re-encryption across ALL lanes (EC/packblob/datawal/
       logical-WAL/FSM-value/IAM/snapshot) — large, may sub-slice; `scrubberKick` is `nil` today
       (`dek_keeper_wiring.go:200`).
     - **S7 slice progress (2026-06-01):** S6a–d rewrap machinery SHIPPED; S7-0 fail-closed prune guard
       SHIPPED (#695, blanket-refuses ALL prune until S7-final). **S7-1 ledger epoch-tag = THIS PR** —
       versions the S6d completion ledger by a lane-set epoch (`CurrentRewrapLaneSetEpoch`, stays 0 = EC+
       packblob; `IsGenFullyRewrapped(gen, nodes, requiredEpoch)`), dormant + behavior-neutral, so a future
       lane that widens the covered set cannot be mistaken as already-done (spec precondition 2). Remaining
       roadmap: **S7-1a write-path MERGED #699 v0.0.496.0** — FSM-value reseal MUTATION via apply-routing: new
       `CmdResealFSMValues{keys}` data-group command (keys are a work-list, NOT ciphertext); apply
       reads-current-and-reseals in the serialized loop → race-free; leader-only per-group chunked
       orchestrator (`DrainFSMValueRewrap`) that **reads keeper-current each iteration** (NOT a pinned gen —
       pinning livelocks under back-to-back rotations + leaks the single-flight guard; code gate caught this)
       and proposes batched reseals (byte+count bounded vs ErrTxnTooBig) until the store is all-at-current;
       `policy:`+`obj:` only, `mpu:` excluded (D4); NOT the synchronous `RewrapLane` interface. **Epoch-neutral**
       (does NOT bump `CurrentRewrapLaneSetEpoch`, does NOT report to the ledger). **S7-1a-2 ledger-path = THIS PR** —
       per-node data-group post-commit callback (`SetOnFSMValueResealDone`, off the apply loop) fired by a
       leader-proposed `CmdFSMValueResealDone` marker (raft-ordered after all reseal batches) → debounced re-Kick →
       `ProposeDEKRewrapProgress`; `FSMValueCheckLane` (read-only RewrapLane: this node's `policy:`/`obj:` all at
       keeper-current? else error → Kick suppresses the report); epoch bumped 0→1 (landed WITH the check-lane).
       **Also fixed a pre-existing EC-scan data-loss-suppressor in `IterECShardScanTargetsAllVersions`**: a
       versioning-on object dual-writes the bare `obj:{b}/{k}` alias + versioned `obj:{b}/{k}/{v}` with the same
       EC-ref meta; the bare alias derived a version-less shardKey (`ecObjectShardKey(key,"")=key`) → shard-not-found
       → EC lane aggregate-error → `Kick` first-error → **completion report permanently suppressed** (this silently
       neutered S6d completion on every versioning-on cluster, dormant only because S7-0 blocks prune). Fix: skip the
       bare alias when `lat:{b}/{k}` exists (the versioned entry covers its shards at `/{k}/{v}/`; pre-versioning
       version-less objects have no `lat:` → kept). Invariant verified: every EC writer mints a non-empty UUIDv7
       versionID, so a live `/{k}/` shard never coexists with a `lat:` key — no coverage lost, no false-positive
       completion. **NEXT: the S6d-reconcile (existing [P2]) must re-drive a meta-raft-lagged follower** — the
       leader-only drain skips a follower whose `activeDEKGen()` still reads the old gen; without reconcile the epoch
       can STALL (liveness, not safety; S7-0 blocks prune + `RetiredGensBelow(follower-active)` excludes the not-yet-
       retired gen, so a transiently-lagged follower is inert). Residual (non-blocking): deposed-leader drain forwards
       to the new leader (idempotent, converges, terminates) — ctx=Background, acceptable.
       **S7-1b datawal checkpoint-GC = THIS PR** — datawal accumulated old-gen segments with no deletion path;
       investigation found its checkpoint is boot-driven (`SaveCheckpoint` only at end of `Recover`) and the only
       roll is gen-driven `RollSegmentOnRotation` (contiguous `lastSeq+1` naming → a sealed segment's exact max
       seq = `nextFirstSeq-1`). Added `GCMaterializedSegments`: after Recover advances the checkpoint, delete
       sealed segments fully ≤ `max(LoadCheckpoint, lastSeq)` (never the active segment); pure delete, no rewrite.
       Bounds datawal disk growth + shrinks the old-gen population S7-final's census gates on. **Boot-driven
       limitation:** runtime-accumulated old-gen segments aren't GC'd until the next restart (S7-0 blocks prune
       meanwhile; a future runtime-checkpoint-advance would relax it). Test-completeness note: the integration
       test proves idempotence + low-segment GC; mid-segment-checkpoint survivor-replay is safe by construction
       (GC's `nextFirstSeq-1 ≤ checkpoint` can't remove an above-checkpoint record) but not directly asserted —
       optional hardening. **Remaining: S7-1b-logical** = logical/PITR WAL needs REWRAP (not delete) within the
       24h PITR retention window (PITR restore replays every segment at its header gen); separate slice; fix the
       stale gen-0 comment in `boot_phases_logical_wal.go`; consider retention-GC past the window. **S7-final
       datawal census term**: block prune of gen G while any remaining datawal segment has header gen==G (cheap
       plaintext header read at [12:16]); this GC bounds that census to checkpoint-ahead + active segments.
       **S7-2** generic `DEKGenSecretStore` reseal+census
       (IAM/JWT/PDP-token/protocred/cluster-config — audit-derived, not hand-enumerated); **S7-final** unified
       predicate replacing S7-0's blanket refuse + ref-safe `Prune` + S4-pin barrier + ⑤ MoveReplica trace.
       Full design: `docs/superpowers/specs/2026-06-01-dek-rotation-s7-prune-safety-design.md` (gitignored).
     - S7 reference-safe `Prune` (DEKKeeper.Prune `safe` arg) + wire `scrubberKick` + rewrap-completion tracking.
       **S6d completion-predicate invariant:** the EC lane's `RewrapByGen` returns `nil` even when
       individual shards (transient `RewrapShardIfStaleAt` error) OR an entire data group (transient
       `CollectECRewrapTargets`/`db.View` error) are skipped — both gaps are log-and-continue and
       invisible to the return value. So S6d's completion predicate MUST NOT treat `RewrapByGen==nil`
       as proof of coverage; it needs an independent per-shard/per-group done-accounting (e.g. probe
       that no shard remains below activeGen) rather than trusting the sweep's error return.
     - [P2] S6d-reconcile: completion reporting is event-driven (rotation post-commit
       kick). A node down for an entire rotation epoch, or whose rotation log entry
       was snapshot-compacted before lanes were ready, never re-reports (conservative-
       safe: delays prune, no data loss). Add a periodic reconcile that re-kicks +
       reports any now-clean generation. (Back-to-back rotations and transient skips
       already self-heal via the full-swept-set report.)
     - [HARD S7 prerequisite — VERIFY] rebalance vs done-flag: if MoveReplica/rebalance
       moves a shard onto a node that already reported gen G done, does it RE-ENCODE the
       shard to active gen (then the done-flag stays valid) or COPY raw sealed bytes (then
       the flag is stale and S7 would prune a gen this node now holds -> data loss)? Trace
       MoveReplica's encode path. If raw-copy: S7 must re-verify residual at prune time OR
       invalidate the per-node done-flag on shard inbound. Do NOT ship S7 prune before
       resolving this.
     - S7 prune ANDs ledger-done with dekRefCounts[gen]==0 AND no in-flight S4-pinned
       encode on gen, AND reads ledger as "done for ENUMERATED lane categories only"
       (EC all-version #692 + packblob gate-3) -- ledger completion is necessary, not
       sufficient.
     - [HARD S7 prerequisite — data-loss class] ledger-done AND dekRefCounts[gen]==0 are
       NOT jointly sufficient: dekRefCounts is incremented ONLY by applyPutObjectIndex
       (object placements). DEK-stamped at-rest data that is neither a rewrap lane NOR an
       object-index entry is invisible to BOTH gates. Known instance: JWT signing-key
       seeds carry a DekGen (meta_fsm_rotation.go) but are not rewrapped on rotation and
       are not refcounted — S7 pruning a gen a live JWT key references makes that key
       permanently unwrappable (auth/token loss). Before S7 prune ships, enumerate ALL
       DEK-gen-stamped at-rest categories (JWT keys, datawal, logical-WAL, IAM, object
       snapshots) and either rewrap each or add it to the prune-safety predicate. Do NOT
       treat "ledger + refcount" as the complete gate.
     - [P3] EC rewrap collect-then-sweep materializes a data group's entire shard-target set into a
       slice inside one `db.View` (`CollectECRewrapTargets` → `IterECShardScanTargetsAllVersions`);
       ctx-cancel is only checked between targets in the lane loop, not mid-scan, and the slice grows
       unbounded. Deliberate (avoids held-txn/goroutine-leak of a channel producer) but a memory +
       responsiveness regression on very large keyspaces — revisit with a batched/streaming cursor
       if a group's object count grows large.
     - (out of epic) raft-log-command plaintext at rest — **separate spec** (DEK is boot-circular).
   - Full re-grounded design in the (gitignored) unified-at-rest-key spec
     (`docs/superpowers/specs/2026-05-28-unified-at-rest-key-hierarchy-design.md`) +
     D-cut bootstrap-envelope design (`...at-rest-dcut-bootstrap-envelope-design.md`).
     See [[project-grains-at-rest-two-key-systems]].

- [ ] **BadgerDB atomic auto-recovery design**
   - Trust risk: recoverable Badger state still requires manual intervention
     during an outage.
   - Signal: recovery plan, applied action, skipped action, and reason.
   - Verification: corrupted/partial Badger replay plus snapshot restore tests.
   - Boundary: design next; implement after recovery journal decision structs
     stabilize.

- [ ] **Rolling upgrade remaining safety slices**
   - Trust risk: mixed-version clusters can hide schema, config, or capability
     divergence behind a healthy surface.
   - Signal: compatibility gates, upgrade status, snapshot-config compatibility
     status, warning/safe rejection/degraded state.
   - Verification: `tests/compat/`, mixed-cluster tests, snapshot-config forward
     policy tests.

- [ ] **Object placement/index orphan and stale reconcile**
   - Trust risk: object-level placement creates a dual-write failure mode between
     data groups and the global object index.
   - Signal: reconcile report, repair/quarantine counters, non-silent failure.
   - Verification: index commit failure injection, orphan data, stale index,
     missing shard, corrupt shard tests.

- [ ] **Cluster health data-group Raft progress**
   - Trust risk: metaRaft can look healthy while a data group is leaderless or
     lagging.
   - Signal: per-group leader, term, lag, drain state, and transfer state.
   - Verification: induced lag, leader transfer, multi-group cluster health e2e.

- [ ] **NFSv4 and 9P auth/access-control correctness**
   - Promote only when `GrainFS` must support untrusted networks, multi-tenancy,
     compliance-driven file access audit, or S3 IAM parity for file protocols.
   - Signal: export-level policy, auth mode, denied access events.
   - Verification: cross-protocol auth tests and reserved `__meta/` namespace
     tests.

## Next

- [ ] **External PDP adapter — deferred slices** (Slice 1 SHIPPED: local-unix-socket-only,
  disabled-by-default, chain/deny-override, fail-closed default + opt-in fail-open,
  admin + protocol-credential paths, Prometheus `grainfs_iam_pdp_*` + `iam.pdp` audit).
  Remaining:
    - Remote `https://` transport + bearer token + SSRF egress filtering — **SHIPPED
      Slice 6** (`http`/`https`-only endpoint, `unix://` removed; DEK-sealed bearer
      token in reserved `iam.pdp.token` via `grainfs iam pdp set-token/clear-token/show`;
      inline `tls.ca_pem` (parity-safe) + TLS 1.2 floor, no InsecureSkipVerify;
      dial-time `net.Dialer.Control` SSRF filter + `Proxy:nil` + `ssrf.allow_private`;
      SSRF-blocked hard-denies regardless of failure_policy). Remaining sub-items:
        - **mTLS client cert** to the PDP — deferred; **necessity to be re-evaluated**
          (likely overkill for an external PDP behind a bearer token; low priority).
        - **per-CIDR SSRF allowlist** — Slice 6 ships only the `allow_private` boolean.
        - **NAT64/DNS64 residual risk** — dial-time IP classification can't detect a
          public-looking IPv6 that synthesizes a route to private IPv4; document +
          rely on operator egress controls (a per-CIDR allowlist would help).
        - **Cross-node DEK-parity e2e** — DEK cross-node replication is verified in
          code + unit-tested (`pdp_token_source_test`), but a tests/e2e cluster spec
          (set-token on node A → PDP consult unseals on node B) was deferred: no ready
          multi-node IAM harness in tests/e2e. Add when one exists.

- [ ] **External PDP Adapter — remaining slices / follow-ups**
    - Decision cache (positive/negative TTL) + grace mode — **SHIPPED Slice 2**
      (`iam.pdp.cache`: ttl_allow/ttl_deny + LRU max_entries + grace_ttl;
      sharded TTL+LRU, stale-preserving lookup, cache cleared on any iam.pdp
      config change, failures never cached, cache-hit audit suppressed).
    - S3/Iceberg data-plane PDP enforcement — **SHIPPED Slice 7 (PR-A #658 metrics
      scope-label + lazy parse-cache; PR-B data-plane wrap)**: `server.policyAuthorizer`
      is now an interface wrapped by a `data_plane`-scope `pdp.Decorator` at the boot
      seam; `iam.pdp.data_plane.enabled` (default false, AND-gated with top-level
      enabled) gates S3 + Iceberg object/bucket authz with deny-override; scope-aware
      req-build (protocol from action prefix, `target_sa=""`, anon auth_method) leaves
      the control-plane wire byte-identical; lock-free `release()` keeps the disabled
      hot path 0-alloc + no exclusive lock. Remaining data-plane sub-items:
        - **NFS / 9P / NBD data-plane PDP** — they use a SEPARATE `s3auth.Authorizer`
          instance (boot_phases_node_services.go); each needs its own wrap + protocol
          RequestContext + e2e.
        - **`SourceIP` into the data-plane RequestContext** — not threaded through the
          S3 `IAMChecker` (server.go:113); pre-existing gap.
        - **Thread request `ctx` through the S3 `IAMChecker`** so the S3 PDP consult
          honors request cancellation (currently `context.Background()`, bounded by PDP
          Timeout). The singleflight result-arm cancel guard already anticipates this.
        - **Per-scope `failure_policy`** — currently one shared knob; `fail_open` opens
          BOTH planes. Needed only if an operator wants data-plane-fail-open without
          control-plane-fail-open.
        - **Per-request deny audit on cache hits** — currently one row per `ttl_deny`
          window (cache-hit suppression); opt-in for high-sensitivity retry monitoring.
        - **Pooled / fixed-`[32]byte` cachekey** to restore zero-alloc on the
          enabled-hit path (touches the shipped Slice-2 cachekey).
        - **Cluster data-plane e2e** — parity is by-construction (common boot seam);
          a multi-node IAM harness for an explicit cluster spec is still absent.
    - **Singleflight** for concurrent duplicate cache misses — **SHIPPED Slice 7 PR-B**
      (`DoChan`, detached callCtx, `sfKey = cacheKey + configGen`, per-waiter cancel via
      select; failures never cached).
    - **Event-driven cache invalidation** (on `iam.pdp` config / policy change) —
      Slice 2 is TTL-only; data-plane analysis (Slice 7 D6) concluded TTL is the only
      correct tool for the external PDP's own drift (GrainFS re-evaluates `inner` every
      request and only caches the PDP consult), so event-driven invalidation is not
      meaningful for the external decision and is dropped, not deferred.
    - Full GrainFS-only audit on the protocol-credential control plane (pre-existing
      gap; Slice 1 added only the PDP-outcome `iam.pdp` audit).
    - Admin peercred/UDS path is intentionally NOT PDP-gated (local socket trust).
    - **`target_sa` is empty for bearer/OIDC credential ops.** The decorator only
      sets `context.target_sa` when the actor principal is a service account; a
      bearer actor minting/rotating/revoking a credential for some SA does not
      expose the target SA to the PDP (the handler has `saID` but
      `AuthorizePrincipal` does not receive it). To close: thread the target SA from
      `authorizeProtocolCredential` into the decorator via request context (e.g. a
      `WithActorTarget` ctx value) — expands beyond the decorator-only boundary into
      `handlers_credentials.go`. Documented as a Slice-1 limitation (spec P2a).
  Spec: `docs/superpowers/specs/2026-05-28-oidc-federated-iam-boundary-design.md`
  "External PDP Adapter — Slice 5/6 Detailed Design".

- [ ] **Zero-CA cutover/revocation follow-ups** (2026-05-29 re-review of the merged
  revocation + complete-cutover slices; zero-CA is greenfield so none of these are
  migration concerns):
    - **[P3] Orphaned join-redirect TODOs (operability)**. A joiner that contacts a
      follower gets `JoinStatusNotLeader` but no leader address to retry against:
      `internal/cluster/meta_join.go:234` `TODO(W7b/W9)` (return
      leader_join_addr/leader_join_spki from member state) and
      `internal/serveruntime/invite_admin.go:96` `TODO(W7b)` (FSM-resolve redirect to
      auto-forward the invite-admin call to the leader). Not a security gap; a
      usability rough edge for multi-node join.
      NOTE: the redirect MUST be "joiner re-dials the leader directly", NOT
      "follower byte-proxies the join stream" — channel binding (RFC 5705
      exporter) is session-local, so a proxy gives joiner and leader different
      exporters and every bound join fails verification.
    - **[P3] GC the durable revoked-node-ID set**. The Zero-CA revoked set (meta FSM
      `revokedNodeIDs`, snapshot `revoked_node_ids` slot 17) grows unbounded, mirroring
      the existing unbounded `revoked_peer_spkis` denylist. Bound or GC once a node-id
      can be provably never-reused. (Deferred from the revoke data-group-evacuation
      work — 2026-05-29.)
    - **[P3] `MoveReplica` self-removal pre-wait is a no-op under v2 raft**. The
      rebalancer self-move path (`DataGroupPlanExecutor.MoveReplica` →
      `waitForLeadershipTransferTarget` → `peerCaughtUp` → `PeerMatchIndex`) reads the
      v2 adapter's `PeerMatchIndex`, which always returns `(0,false)`
      (`raftnode_adapter.go`), so the pre-wait can never observe catch-up and times
      out — the same latent bug the evacuation slice fixed in `EvacuateVoter` by
      calling `TransferLeadership()` directly (v2 §3.10 picks the most-caught-up peer
      from the leader's real matchIndex and steps down regardless). Apply the same
      direct-call fix to `MoveReplica` self-removal and retire
      `waitForLeadershipTransferTarget`/`peerCaughtUp` if no caller remains. (Surfaced
      by the revoke data-group-evacuation e2e — 2026-05-29.)
    - **[P3] Optionally make `ProposeShardGroup` itself forward to the meta-leader**
      (option A), repairing the pre-existing latent gap where a non-meta-leader
      data-group leader cannot converge the PeerIDs mirror. The evacuation path uses
      the new `ProposeShardGroupForwarding` instead; option A was verified
      apply-wait-equivalent but deferred on surgical grounds.
    - **[P3] Revoke eviction is best-effort; a 2-voter group whose other voter is
      revoked cannot self-heal**. After revoke `ClosePeer`s the revoked node, a data
      group with exactly `[revoked, survivor]` cannot commit either AddVoter or
      RemoveVoter (the old 2-voter config needs a quorum that includes the now-
      unreachable revoked node — raft joint-consensus limitation). The evacuator
      logs `eviction failed; retry next tick` and the revoked node stays in
      `raft_voters` until an operator intervenes or the cluster key is dropped
      (hard-security path). Groups with RF≥3 evict cleanly. The guarantee here is
      AVAILABILITY (eventually-consistent), not hard security. Consider a
      force-shrink / operator command for the stranded 2-voter case. (Surfaced by
      the revoke data-group-evacuation code gate — 2026-05-29.)
    - **[P3] Evacuation discovery + MoveReplica remove assume node-id PeerIDs**.
      `DataGroupEvacuator.ledTargets` compares raw `dg.PeerIDs()` against the node-id
      revoked set, and `MoveReplica` removes by `fromNode` (node id). True for Zero-CA
      greenfield (`seedShardGroupPeerIDs` emits node ids), but if legacy address-form
      shard peers are ever introduced, ledTargets would miss them and MoveReplica's
      remove would not match the config. `EvacuateVoter` itself already normalizes via
      `ResolveShardGroupPeer` and removes by the raw Server.ID, so the remove-only path
      is robust; add the same normalization to ledTargets discovery + MoveReplica if
      address-form peers become possible. (Surfaced by the code gate — 2026-05-29.)

- [ ] **Auth redesign §1 Foundation post-ship cleanup** (v0.0.260.0 review-forever
  Pass 1 INFO findings — non-blocking, ship after §2/§3 to keep blast radius small):
    - Maintainability M#2: extract `peelTrailer(data, magic, trailerLen)` helper in
      `meta_fsm.go` (3 structurally identical DKVS/GCFG/IAM peel blocks).
    - Maintainability M#4: rename `encrypt.LoadFromFSM` → `NewDEKKeeperFromVersions`
      (caller-context leaking into callee package name).
    - Maintainability M#6: extract common pattern from `applyConfigPut` /
      `applyConfigDelete` (nil-guard + decode + dispatch).
    - Maintainability M#7: drop the `safe bool` param on `DEKKeeper.Prune` —
      callee should expose `PruneUnchecked` and the FSM guard does its own
      ref-count check before calling.
    - Maintainability M#8: drop the `snapshotMetaFSMToBytes` /
      `restoreMetaFSMFromBytes` one-line wrappers in `config_snapshot_test.go`.
    - Maintainability M#10: register `encryption.rotate-dek` /
      `prune-dek-version` with `OnTrigger: nil` instead of no-op closures.
    - Testing F#11: end-to-end `ProposeConfigPut → Apply → cfgStore.Set → hook`
      test through a real `MetaRaft` (only unit-level apply path covered today).
    - Testing F#12: `applyConfigPut` with a corrupt inner FlatBuffer payload
      (decode-error gate currently untested).
    - Performance P#8: `Snapshot()` acquires 3 separate locks (FSM mu, cfgStore mu,
      DEKKeeper mu) in sequence — document the single-writer guarantee or fold
      into one acquisition if snapshot latency becomes measurable.
    - Tracking: `docs/superpowers/plans/2026-05-19-auth-redesign.md` §10 R-notes
      + §11 decisions log.

- [ ] **§9 Follow-ups** (from F#41/F#41b implementation):
    - **F#42 (Pass-1 MEDIUM-1)**: Phase 0 anon Allow paths emit no audit row
      (`request_authz.go:148` gates recordAllow on `AuthEnabled()`). Phase 2
      anon-to-default IS audited via `AnonAllow` flag. Phase 0 long-lived state
      needs a forensic trail — anon writes to /default during Phase 0 should
      produce at least a single coarse audit row per request. Trade-off: audit
      table volume vs. forensic capability.
    - **F#44 (Pass-1 LOW-2)**: T73 cluster prober runs ~12 samples vs ~11811 on
      single-node (QUIC forward saturates). Parallelize cluster prober (5-10
      goroutines) to lift sample density, OR document the cluster-density gap
      so a future maintainer doesn't assume burst coverage.
      `tests/e2e/phase_transition_test.go:170-201`.
- [ ] **Auth redesign DX follow-ups** (from `docs/superpowers/specs/2026-05-19-auth-redesign.md`
  Codex review, medium+cosmetic tier). All single-PR-sized, ship after the main
  redesign lands:
    - [ ] `grainfs iceberg secret duckdb --warehouse X --sa Y` and `grainfs
      iceberg catalog spark|trino --warehouse X --sa Y` — emit copy-paste config
      blocks per client. Reduces per-warehouse-token UX tax.
    - [ ] `grainfs iam jwt-key rotate --auto-prune-after-ttl` — scheduler-friendly
      one-shot rotation that runs prune after the TTL window elapses. Optional
      systemd timer / k8s CronJob example in operator docs.
    - [ ] `grainfs doctor auth --warehouse X --sa Y` — health check that walks
      the entire auth path (TLS reachability, socket access, SA exists, policies
      attached, JWT key state, bucket exists, client URL base sane) and prints
      the first broken link.
    - [ ] `docs/users/minio-muscle-memory-map.md` — `mc admin policy info` →
      `grainfs iam policy get`, `mc admin user add` → `grainfs iam sa create`,
      `aws s3api put-bucket-policy` → `grainfs iam bucket policy put` (admin
      UDS), `mc console` → (no equivalent yet). Per-row "why different" column.
    - [ ] Policy template library at `docs/users/iam-policy-templates/`:
      `readonly-bucket.json`, `readwrite-bucket.json`, `write-prefix.json`,
      `read-prefix-from-cidr.json`, `iceberg-readwrite-warehouse.json`. Each
      template is a starting JSON with comments + a sed-friendly bucket name
      placeholder. Wired into `grainfs iam policy create --from-template <name>
      --bucket <b>`.
    - [ ] Adjust spec/docs language: "AWS IAM JSON subset" everywhere instead of
      "AWS-IAM-compatible." Banner block in `docs/users/iam-policy-from-aws.md`
      enumerating the unsupported constructs (`NotAction`, policy variables,
      most condition keys, inline policies, account principals).
    - [ ] Built-in policy attach warning surface (covered in spec, but TODO is
      the docs sentence — explain in `docs/users/iam-policy-from-aws.md` why
      `readwrite` is "global-readwrite" semantically and link to the per-bucket
      template.
    - [ ] `grainfs doctor snapshot` — preflight check that operators run on an
      old binary before installing a new one. Walks the meta-FSM snapshot,
      reports schema version + compatibility with a target binary version.
      Mitigates the "fail-loud cluster won't start" surprise for private
      cluster evaluators.
    - [ ] `docs/operators/containerization.md` — exact chmod/chown/socket-dir
      behavior for admin UDS under rootless containers, systemd units, mounted
      sockets, sidecars, CI users. Failure messages spelled out so operators
      can grep them.
    - [ ] Error-message SA-ID exposure decision — keep current "SA <id> lacks
      ..." or replace external surface with `access_key_suffix` (last 4 chars),
      moving full SA id to audit log only. Needs explicit ADR.
    - [ ] External Iceberg client first-tier-only docs — keep warp + DuckDB as
      tested examples; demote Trino/Spark/PyIceberg/Flink to "configuration
      pattern, not e2e verified" until each gets its own e2e cell. Avoids stale
      docs claiming compatibility we haven't measured.
- [ ] **NFS `rdattr_error` required gap**: implement READDIR per-entry
  attribute error semantics.
- [ ] **pynfs/nfstest conformance matrix**: publish nightly/basic suite results
  as an operator-readable pass/fail matrix.
- [ ] **Hot reload drift detection**: detect disk/runtime config mismatch after
  config reload.
- [ ] **Migration mirror/cutover correctness**: settle mirror, cutover, and
  status semantics before dashboard polish.
- [ ] **Bucket/object-lock/retention governance design**: promote only after the
  semantic mapping is clear for version-level retention records,
  governance/compliance delete and overwrite behavior, legal hold permissions,
  lifecycle expiration interaction, and NFS retention attribute alignment.
- [ ] **Scrub dedup 영구성 정책**: 현재 `scrubber.Director.dedup`은 영구.
  운영 의도 확인 후 done/cancel 시 cleanup 명령 추가 (ADR 또는
  grill-with-docs). 참조: `docs/architecture/scrubber-director-actor.md`
  결정 5.
- [ ] **공통 JobActor 추상화 검토**: `scrubber`/`lifecycle`/`migration`
  worker가 공유 가능한 lifecycle 패턴. design doc의 JobActor 컨셉 참조.
- [ ] **Scrub Register boot phase constructor 옵션화**: `BlockSource`/`Verifier`를
  `NewDirector` 옵션으로 주입해 `Register`/`started.Bool` 가드 제거.
  boot phase 의존 그래프 재배치 동반.
- [ ] **Cluster local pre-check audit (post-SetBucketVersioning fix)**:
  `DistributedBackend`에 `b.HeadBucket`/`b.HeadObject` 후 `b.propose`
  패턴이 15곳 남아 있다 (`SetObjectACL`, multipart paths, etc.).
  follower가 meta-Raft bucket assignment를 받았지만 data-Raft
  `CmdCreateBucket`을 아직 apply하지 않은 짧은 윈도우에서
  같은 false-`NoSuchBucket`을 반환한다.
  `ClusterCoordinator` 단에 cluster-aware pre-check + base에
  `*ProposeOnly` entrypoint 패턴을 일관 적용. 참조 커밋:
  `fix(s3auth/cluster): warp versioned workload passes on a 4-node
  cluster` (`benchmark` branch).
- [ ] **Capability evidence warmup wiring**: `/v1/cluster/capabilities` now
  exposes gate evidence on the admin UDS, so the original ready-probe gap is
  closed. Remaining work is to replace fixed benchmark/operator sleeps
  (`CLUSTER_WARMUP_SLEEP=45` in `docs/reference/benchmarks.md`) with polling
  against that endpoint, then re-check whether gossip latency still needs
  interval tuning or raft-committed evidence.

## Deferred Until Triggered

- [ ] **At-rest cluster-key PSK: distinguish rebind-rewrap from migrate-rewrap** [P3].
  Slice 2 (cluster-key PSK protection) shipped, but the `KeyProtector.Unprotect`
  contract collapses two cases into one `rewrap bool`: a REBIND (value came from a
  successfully-opened recovery slot → trustworthy) and a MIGRATE (raw passthrough of
  a non-container blob → untrustworthy). Consequence (code-gate MEDIUM): in env mode,
  a container whose 4 GKEK magic bytes are corrupted (but AEAD slots intact) fails
  `LooksLikeEnvKEK` → takes the legacy-raw branch → `ResolveClusterKey`
  (`clusterkey.go`) persists the ~260-byte garbage via WriteCurrent on `hasDisk &&
  rewrap` with no validate-before-persist → destroys the original slot, wrong identity
  fails LATER at the QUIC handshake (not loud at boot). Narrow (needs env + corruption
  hitting exactly the magic bytes). NOT fixable by `ValidateClusterKey` (length-only,
  passes 260 bytes) NOR a hex gate (operator `--cluster-key` accepts non-hex 64-char
  keys per psk.go — a hex gate would narrow the accepted key space = behavior change).
  Correct structural fix: split `Unprotect`'s `rewrap` into rebind vs migrate (a
  Slice-1 seam change touching the KEK path too) and only persist-on-rebind freely;
  treat migrate as needing explicit validation. The KEK lane is immune by construction
  (`KEKStore.Add` rejects `len!=32` BEFORE the rewrap-persist); the PSK lane has no
  such exact-length invariant.
- [ ] **M1 plaintext-guard latent footgun** [P3]. `keystore.go readSlotRewrap` fails
  loud when a plaintext-protector keystore reads GKEK-magic bytes. Impossible today
  (hex/random keys can't start with "GKEK"), but if PSK validation ever loosens to
  accept arbitrary strings, a PSK literally starting with `GKEK` would make a node
  unable to read its own key. Covered for free if the rewrap-split above adds a hex/
  format gate.
- [ ] **At-rest KEK protection Slice 3 — KMS/HSM/TPM providers** [P3]. Implement
  concrete `KeyProtector` backends behind the existing seam (envelope: external
  service wraps our KEK). No interface change; adds external SDK deps.
- [ ] **env KEK protector: secret-resident residual** [P3]. `--kek-protector=env`
  enforces the recovery secret at boot (`buildKEKProtector`), but
  `EnvProtector.Protect` re-resolves it at KEK-rotation time. If an operator
  unsets `GRAINFS_KEK_RECOVERY_SECRET` after boot, a later rotation's
  `AddAndPersist`→`Protect` could fail and fatal-halt the node. The boot gate
  narrows but does not fully close this; documented as "secret must stay resident
  for the service lifetime". Consider caching the boot-resolved secret in the
  protector so rotation never depends on live process env.
- [ ] **Flaky `TestEncryptedObjectFileReadAtRejectsCorruptRequestedChunk`** [P3]
  (`internal/storage/encrypted_object_file_test.go`). ~3/10 FAIL on master base
  (devel d217cdc8), unrelated to KEK-protector work — the corrupted-chunk
  full-read fallback intermittently returns nil error with 1024 bytes.
- [ ] **Optional `grainfs encrypt kek unprotect` admin tool** [P3]. Enabling
  `--kek-protector=env` is a one-way migration (plaintext reader can't parse the
  container). Add a tool to convert env→plaintext if reversibility is needed.
- [ ] **Retire dead `internal/encrypt/filekek.go`** [P3] (`FileKEK`) — no
  production callers; superseded by KEKStore + KeyProtector.

- [ ] **KEK-envelope C-prune-followup: `SegmentRef.dek_gen` done right + with consumer**.
  Deferred from the D-seg-ec-activate slice (v0.0.368.0). Recording the sealing DEK
  generation in segment metadata was cut because the only cheap source
  (`keeper.Active()` at segment-write time) is not guaranteed to equal the actual
  per-shard seal gen (gen-pinning is per-shard-stream; rotation-mid-write / remote-node
  differences), so a recorded value could be silently wrong — a footgun for a prune
  consumer that trusts it to drop DEK generations (wrong-low → prune a still-referenced
  DEK → unreadable data). Authoritative per-shard gen already lives in the GFSENC3 header.
  When reopened: thread the REAL seal gen out of the shard write path through
  `storage.SegmentRef` + `storagepb.SegmentRef` + `clusterpb.SegmentRef` +
  `clusterpb.SegmentMetaEntry` + PutObjectMeta/CompleteMultipart codecs +
  `segmentMetaEntriesToRefs`, AND build the prune consumer that reads it (cross-checking
  the GFSENC3 header gen). Bundle both so the recorded value is correct before anything trusts it.
- [ ] **KEK-envelope: write-path `ErrDEKGenUnknown` → retriable 503**. On the EC-shard
  PUT path, `cpupool.go` → `commit.go` collapses a per-shard `encrypt.ErrDEKGenUnknown`
  (gen not yet local) into a generic "K shards unreachable" error → likely a 500 on S3
  PUT. `WaitDEKReady` (run.go:227) gates serving so a normal write never hits an empty
  keeper, making this unreachable on a serving node today. Reopen as a hardening pass:
  detect `errors.Is(err, encrypt.ErrDEKGenUnknown)` in the commit coordinator and map it
  to a retriable 503 (not 500). The READ side already classifies it as transient (slice C).
- [ ] **InstallSnapshot Restore failure should fatal-halt, not log-and-advance**.
  `meta_raft.go` apply-loop `LogEntrySnapshot` case logs a `Restore` error then
  advances `lastApplied` to the entry index regardless. A joiner that receives an
  InstallSnapshot it cannot open (now reachable via D-snap envelope-open failure:
  unknown KEK version, wrong cluster, or corruption) continues with un-restored FSM
  state but an advanced applied index → silent divergence. Pre-existing pattern;
  D-snap adds a new crypto failure mode to it. Surfaced by /review adversarial pass
  (2026-05-28). Fix: treat envelope-open / Restore failure on InstallSnapshot as a
  fatal halt (mirror the existing `ErrFSMKEKFatal` path) rather than log-and-advance.
- [ ] **Phase D-snap D-cut (object)**: remove `snapshot.openSnapshotBlob` legacy
  plaintext passthrough and add a boot-time scan that refuses startup if any
  plaintext snapshot file remains. The `grainfs_snapshot_legacy_plaintext_reads_total`
  counter is a runtime signal, not sufficient alone. Mirrors meta-FSM D-cut.
- [ ] **KEK prune-refusal: absolute closure of the in-flight snapshot-write window [P3]**.
   The prune guard scans retained `.json.zst` + in-flight `.json.zst.tmp` and uses the
   APPLIED raft index for attestation freshness, which closes the race to a
   sub-millisecond in-memory-seal window (a `Create()` that captured the retiring KEK
   version but has not yet written its `.tmp` while the same node has already applied
   retire). For absolute closure, `snapshot.Manager.Create` could acquire a short
   `KEKLeaseTracker` lease on the sealed version across seal+rename, so in-flight writes
   surface as `lease_count > 0`. Very low priority — the current window is practically
   unreachable.
- [ ] **KEK-envelope: cluster e2e join + snapshot-restore object reads**. The
   D-seg-ec-activate e2e added rotate-survives + follower-read-no-quarantine (both green
   on a live 3-node cluster). Join-after-bootstrap and snapshot-restore-boot object-read
   specs were skipped because the e2e harness has no dynamic `AddNode` (4th node post-
  bootstrap) and no snapshot-restore-boot helper (it has `KillNode`/`RestartNode` only).
  Reopen: add an `AddNode`/post-bootstrap join helper + a snapshot-restore-boot helper to
  `tests/e2e/cluster_harness_test.go`, then add the two additive specs under the "KEK
  rotation lifecycle" Describe.
- **KEK-envelope: DataEncryptor buffer-reusing seam — remaining consumers.** The `SealTo`/`OpenTo`
  seam methods (+ `encrypt.AppendAAD`, `DEKKeeper.SealWithAADTo`/`OpenWithAADTo`,
  `TransientReadOnlyDEK.OpenWithAADTo`, pooled `withSeamAAD`/`withSeamAADErr2`) exist and are wired
  through all 3 adapters; packblob `Append` (Seal), spool `Read` (Open), spool write (Seal), the
  EC-shard readers (`eccodec/shardio.go` Open), the single-node object readers
  (`encrypted_object_file.go` Open), and the `AADField` per-field construction (inline values, no
  per-field `make`) consumers are migrated.
  Open side needs **per-consumer lifetime analysis** — Open plaintext escapes to callers, so pooling the
  `OpenTo` dst is a use-after-free hazard, NOT a mechanical pool reintroduction. Each remaining consumer
  is its own slice; bench ≥15s×3 (allocs/op AND B/op).
  - packblob `Read` — Open side; needs `OpenTo` + lifetime analysis. Plaintext **escapes** to the S3
    read path / Compact `entries` (UNSAFE for naive pooling) — needs ownership rework, not a mechanical
    OpenTo swap. **The last remaining hot-path Open consumer.**
  - datawal (`scanRecords`) Open — **dropped**: cold path (WAL recovery/startup only), and the plaintext
    is copied (`copyBytes=true`) + cleared per record, so `OpenTo` buffer reuse yields no real benefit.
  - The `[]AADField` slice itself (one per builder call, e.g. `ShardAADFields`/`chunkFields`) still
    allocates — pooling it is non-trivial (concurrency requires a fresh slice) and is a separate larger
    refactor, not pursued. The per-field `make` floor it sat on is now gone.
- [ ] **KEK-envelope D-wal: live DEK rotation segment rollover [P1]**.
  D-wal-data now opens production writer/recovery paths with `DEKKeeperAdapter`
  and new encrypted `internal/storage/datawal` segments probe-seal before header
  write so `dek_gen` records the actual active generation. The rotation-boundary
  method `WAL.RollSegmentOnRotation` is now SHIPPED (see below). Rotation is still
  deferred because the method has no production caller: it must be wired
  SYNCHRONOUSLY on DEK rotation (a currently-open segment pins one generation while
  `DEKKeeperAdapter.Seal` uses the live active gen, so appends fail-closed until the
  segment rolls). Remaining before enabling `encryption.rotate-dek`: synchronous
  wiring (S5) + the legacy-WAL boundary (RESOLVED below).
  **Legacy-WAL caveat — RESOLVED (S3, #678).** `internal/storage/wal` now uses a
  seal-first write path: the rotation cause of the async-drop is closed (a DEK rotation
  rolls the segment instead of erroring into `writer()`'s log+continue). Two residuals are
  split out as their own follow-ups below: the crash-window boot/PITR brick and the
  hard-IO-error silent drop.
  **Plan-gate findings (2026-05-29, a `RollSegmentOnRotation` slice was scoped then
  deferred — fold these into the implementation here so they are not re-discovered):**
  - **`RollSegmentOnRotation` method — SHIPPED (this PR).** Empty current segment →
    `Truncate(0)` + re-`initSegment` in place; non-empty → `Sync`+`Close` then
    `O_CREATE|O_EXCL` a NEW file named for the next firstSeq (zero-padded, sorts after
    the prior segment; `O_EXCL` fails closed against the double-header collision class).
    No-op when the freshly probed gen equals `w.dekGen`; `w.lastSeq`/`w.lastTimestamp`
    preserved (seqMonotonic). Mirrors `Close()`'s `isSyncing` wait-guard so a concurrent
    group-commit `Flush` cannot Sync a closed fd. No production caller yet.
  - **Wiring must be SYNCHRONOUS, not async post-commit.** The keeper's active gen
    advances synchronously in the FSM apply, but `handleDEKReplicatedRotate`
    dispatches via `go` (must-not-block) — between gen-advance and an async roll,
    every `Append` seals under the new gen into an old-gen-pinned header → the
    `wal.go:148-153` assertion fails (transient write errors). Also `state.dataWAL`
    is nil at `WireDEKPostCommit` time (opened later in the storage phase), and the
    hook early-returns when `scrubberKick == nil` (today's prod state). Use a
    synchronous roll (or roll-and-retry inside `Append` on gen mismatch) reachable
    after `state.dataWAL` exists.
  - **Un-gate gate:** enabling `encryption.rotate-dek` is blocked until BOTH (a) the
    datawal rollover boundary is wired (S2 method shipped #676; synchronous wiring at S5)
    AND (b) the legacy-WAL non-dropping boundary — **(b) DONE (S3, #678).** Legacy WAL
    shares the same gen-aware seam; (a) still pending.
- [ ] **datawal `isSegmentName` 10-digit cap [P3]** (pre-existing, not introduced by S7-1b).
  `isSegmentName` requires exactly 10 decimal digits (e.g. `datawal-0000000001.bin`). At
  seq ≥ 10^10 (ten billion) the filename grows to 11 digits, `isSegmentName` returns false,
  and the segment is invisible to `segmentFiles` / `GCMaterializedSegments`. Additionally,
  lexicographic sort diverges from numeric order at 11 digits, so "active = files[last]"
  breaks. The WAL cannot roll past seq 9,999,999,999 safely. Fix: use a variable-width
  format with leading zeros only up to the max expected width, OR switch `isSegmentName` to
  a prefix+suffix check with `strconv.ParseUint` validation instead of a fixed-length check.
  Captured here per S7-1b plan-gate fix #4.
- [ ] **WAL (legacy + datawal) rotation crash-window durability [P1]** (surfaced by the S3
  plan-gate; pre-existing, affects the plaintext WAL too). `rotate` creates `wal-<seq>.bin`
  then `writeHeader` with **no fsync** — a crash in between leaves a zero-length / torn-header
  segment **visible under a scanned name**. On restart `scanMaxSeq` (`internal/storage/wal/wal.go`
  encrypted branch returns the read error) turns that into a **boot failure**, and strict
  `replay`/`ReplayEncrypted` into a **PITR-restore failure** (the #672 torn-tail tolerance covers
  v4 *body* short-reads, NOT the header window). S3 does not make this worse (it dropped the
  proposed `O_EXCL`, preserving the `size==0`/header-only self-heal) but does not close it. Fix:
  write the new segment to a tmp name → fsync → atomic rename (a partial segment is never visible
  under a scanned name); a half-measure fsync-after-header does NOT close the window (the file is
  visible zero-length from `O_CREATE`). Pair with fsync-on-rotation-boundary + a gap-detection
  signal (`seqMonotonic` only rejects `seq <= prev`, so an ascending gap is invisible today).
- [ ] **WAL hard write-error non-dropping policy [P2]** (surfaced by the S3 plan-gate). After S3,
  a `writeEntry` failure from a hard I/O error (disk full, EIO) — not a DEK rotation — still
  log+continues in `writer()` / the channel-full sync fallback (`internal/storage/wal/wal.go`)
  while `lastSeq` already advanced → a silent seq gap. S3 closed the **rotation** cause of this
  drop; the I/O cause remains. Fix: halt / fail-close the WAL on an unrecoverable write error
  instead of advancing past a lost entry. Applies to both legacy `wal` and `datawal`.
- [ ] **packblob `Compact` active-blob concurrency hardening [P2]**. Pre-existing race
  (surfaced by codex during D-seg-pack review, not introduced by it): `Compact` reads the
  source blob without `bs.mu` (`internal/storage/packblob/blob.go` ~440), only later locks
  to rotate if compacting the active blob (~522), then `os.Remove`s the old file (~542). A
  concurrent `Append` can land in the blob after the read pass but before rotation/removal,
  then `Compact` unlinks it. Reopen: forbid compacting the active blob, or lock+rotate
  before scanning. Add a concurrent Append-during-Compact regression test.
- [ ] **S3 Range GET residual p95/p99**: reopen when product/SLO requires p95
  below 25 ms and p99 below 35 ms, or when FUSE/s3fs/goofys random reads
  reproduce EC read amplification.
- [ ] **S3 Range GET 1 MiB full-width bottleneck**: reopen only with new pprof
  evidence for shard-side encrypted range decrypt or QUIC syscall cost.
- [ ] **NBD direct-write bottleneck**: reopen when direct I/O write SLOs matter
  or trace-off profiles confirm per-block `open/pwrite` as the p95 bottleneck.
- [ ] **NBD qemu/libnbd interop**: promote `OPT_INFO`/`OPT_GO`,
  `NBD_INFO_BLOCK_SIZE`, structured replies, block status, and extended headers
  only after a disposable qemu/libnbd smoke harness runs against a real GrainFS
  NBD port.
- [ ] **Iceberg REST high-concurrency Raft ceiling**: reopen when production
  catalog workloads miss SLOs or the consistency spec for reducing proposals is
  clear.
- [ ] **Iceberg Spark/Trino/PyIceberg client coverage**: promote only after
  real-client REST Catalog smoke tests define which client behaviors are
  supported versus DuckDB-only compatibility.
- [ ] **Volume CLI follow-ups**: export/import, policy, attach/detach, and rename
  need concrete server-side lifecycle requirements.
- [ ] **Scrub scope, EC scrub race, group dir cleanup, PeerHealth threshold**:
  reopen when their telemetry triggers fire.
- [ ] **Incident store scope index / `ScanObjects(bucket, keyPrefix)`**: reopen
  when measured margins fail or a concrete caller needs prefix scope.
- [ ] **Protocol credential remaining real-client smoke coverage**: protocol
  credential data-plane enforcement is wired for S3, Iceberg, NBD, NFS, and 9P
  server paths. S3 now has MinIO `mc` single-node and cluster real-client smoke
  coverage; Iceberg now has DuckDB REST Catalog single-node real-client smoke
  coverage. Follow up by adding Iceberg cluster real-client smoke once the
  static MR cluster fixture is stable, plus NBD/qemu-libnbd and NFS/9P
  credential mount smoke coverage before promoting broader compatibility claims.

## NFSv4 RFC 8881 Follow-Ups

### Required

- [ ] [nfs-audit] bit 11 `rdattr_error` [P0] [Skipped]: READDIR does not emit
  per-entry attribute errors; operation-level errors only. Owner: TBD.

### Recommended

- [ ] [nfs-audit] bit 12 `acl` [P2] [Skipped]: implement NFS ACL payloads if a
  policy requires them. Owner: TBD.
- [ ] [nfs-audit] bits 16/17 case flags [P2] [Skipped]: return
  case-sensitive/case-preserving booleans. Owner: TBD.
- [ ] [nfs-audit] bits 21/22/23 inode-style file counts [P2] [Skipped]: add only
  after defining synthetic capacity policy. Owner: TBD.
- [ ] [nfs-audit] bit 24 `fs_locations` [P2] [Skipped]: referrals and
  `NFS4ERR_MOVED` need a design. Owner: TBD.
- [ ] [nfs-audit] bits 36/37 owner/group [P1] [Partial]: design idmap or
  authenticated user mapping. Owner: TBD.
- [ ] [nfs-audit] bits 38/39/40 quota [P2] [Skipped]: design NFS quota
  accounting before implementation. Owner: TBD.
- [ ] [nfs-audit] bits 42/43/44 filesystem capacity [P2] [Skipped]: select a
  capacity source before surfacing values. Owner: TBD.
- [ ] [nfs-audit] bit 48 `time_access_set` [P1] [Partial]: persist atime in the
  sidecar schema. Owner: TBD.
- [ ] [nfs-audit] bits 56/57 directory notifications [P2] [Skipped]: design
  notification behavior before implementation. Owner: TBD.
- [ ] [nfs-audit] bits 58/59 DACL/SACL [P2] [Skipped]: requires the ACL/audit
  model. Owner: TBD.
- [ ] [nfs-audit] bits 62-68 pNFS [P2] [Skipped]: out of scope unless `GrainFS`
  adds pNFS support. Owner: TBD.
- [ ] [nfs-audit] bits 69-73 retention metadata [P2] [Skipped]: map only after
  object-lock semantics are defined. Owner: TBD.
- [ ] [nfs-audit] bit 74 masked mode SETATTR [P2] [Skipped]: implement if
  client workloads require it. Owner: TBD.
- [ ] [nfs-audit] bit 76 charset capability flags [P2] [Skipped]: add after UTF-8
  policy is explicit. Owner: TBD.

## Chunking / Large-Object Follow-Ups

- [ ] **Cluster pull-through large-object parity [P1]**: `TestPullthroughE2E/
  Cluster4Node/LargeObject` (5 MiB random payload) returns bytes that differ
  from the upstream payload after the cache-miss GET; the cache-hit GET
  exhibits the same divergence. SingleNode passes the identical case. The
  symptom looks like truncation or a partial 2-pass streaming write to the
  cluster's local cache before the response is served. Test was originally
  single-only; promoting the e2e to TestBucketsE2E dual surfaced this gap as
  a now-failing Cluster4Node subtest — the failing assertion is the
  regression signal that unblocks closing the gap. Fix candidates: trace the
  pull-through 2-pass streaming write on cluster (where in EC distribute the
  body is consumed) and ensure the local cache write completes before the
  HTTP response body is closed.

- [ ] **PITR WAL replay carries segment metadata [P1]**: snapshot/restore now
  knows `Object.Segments`, but `storage/wal.Entry` still only carries scalar
  object metadata (`Bucket`, `Key`, `ETag`, `ContentType`, `Size`, `VersionID`).
  WAL-only PITR replay can therefore recreate a segment-backed object record
  without the segment refs and later report it stale or unreadable. Add a WAL
  format version that encodes segment refs, preserve old replay compatibility,
  and add PITR coverage for a chunked object created after the snapshot point.

- [ ] **Placement monitor: repair a corrupt LOCAL shard from peers instead of quarantining [P2]**:
  `scanRecord` now correctly classifies confirmed corruption (CRC/structural/AEAD) and
  still quarantines the **whole parent object** at object granularity — the on-corruption
  semantics were intentionally left unchanged in the transient-classification PR. But a
  single corrupt local shard is usually recoverable: with 4+2 EC and enough surviving
  peers, reconstruct the bad shard in place (same path the missing-shard repair already
  uses) and only quarantine if reconstruction fails. This downgrades most corruption
  events from object isolation to a silent local repair. (`internal/cluster/shard_placement_monitor.go`.)

- [ ] **Placement monitor: stream scan targets instead of buffering O(objects+segments) [P3]**:
  `Scan` buffers all `ECShardScanTarget`s before processing; ~1.5 GB peak for 1 M chunked
  objects × 10 segments each. Follow up if production scans show RAM pressure; streaming
  would hold the FSM read transaction open during local shard checks (its own tradeoff).

- [ ] **Placement monitor: scan non-latest object versions' segment/coalesced shards [P3]**:
  The placement monitor currently covers only the latest object version
  (`IterObjectMetas` semantics). Segment and coalesced EC shards belonging to
  non-latest versions are not proactively scanned; they rely on read-time EC
  reconstruction. Extend the monitor to iterate non-latest versions' segment refs
  as well, closing the between-boot gap for versioned objects with degraded
  older-version shards.

- [ ] **AppendObject real per-call MD5 chain [P2]**: `AppendObject` still stores
  segment xxhash checksums in `AppendCallMD5s` as a stopgap. Capture each append
  call payload's real MD5 at the API/storage boundary and persist that digest in
  both single-node and cluster apply paths so composite append ETags are S3-wire
  correct while segment checksums remain xxhash3 for internal integrity.

## AppendObject Follow-Ups

- [ ] **Forward buffer 512 MiB warp calibration [P1]**: production traffic
  pattern에 default `--cluster-append-forward-buffer-total-bytes` 512 MiB가
  적정한지 검증. `warp append --concurrent 32 --duration 60s --obj.size '1-16MiB'`
  실행, `grainfs_cluster_append_forward_buffer_rejected_total` ratio < 1%
  확인. 충족 못 하면 default 상향 또는 memory-budget tuning을 separate PR로
  분리. PR description에 measurement result 또는 미실행 사유 명시.

- [ ] **EC shard orphan cleanup [P2]**: PR #425 (raw segment side)에서 미해결.
  coalesce 도중 EC 쓰기 후 propose 실패 시 남는 EC shard dir
  (`<shardRoot>/<bucket>/<userKey>/coalesced/<id>/coalesced/<id>/shard_<i>`)
  cleanup. 기존 `OrphanWalkable.WalkOrphanShards` 인터페이스는 plain EC만
  cover — `ScanObjects`가 `lat:` 인덱스로 coalesced shardKey를 yield하지 않아
  known set 구축이 안 됨. Storage layout (shardRoot vs data root) + coalesced
  shard tracking mechanism 조사 → `OrphanWalkable` 확장 또는 별도 sweep
  메커니즘 도입.

- [ ] **Coalesce recoalesce depth audit [P2]**: design open question — 새
  raw segment가 다시 threshold 도달 시 또 coalesce하면 `coalesced[]`에 entry가
  계속 누적된다. `MaxCoalescedEntries=1024` cap 외에 measurement-driven 정책
  (max depth, periodic 통합) 검토. v0.0.253.0 Hardening이 추가한
  `AppendCoalescedEntriesAtCap` counter로 production cap-reach 빈도 baseline
  수집 후 판단.

- [ ] **`AwaitWriteFromNonOwner` harness EC-aware 강화 [P2]**: 현재
  AwaitWriteFromNonOwner는 healthy-cluster path 전용 — EC stripe width ==
  cluster size 환경에서 owner kill 후 호출하면 모든 PUT이 ServiceUnavailable.
  PR #424 T24 OwnerKillSurvives는 직접 `/api/cluster/status` `leader_id`
  폴링으로 우회. EC degraded write을 인식하는 helper 또는 별도 RotationSettled
  helper 분리 검토.

## Pre-existing Test Failures (Phase B3 무관)

- [ ] **`protocred.TestStoreRestoreIsDetachedAndPreservesNoPlaintextSecret` cross-package
  flake [P3]**. FAILs intermittently in the full parallel `make test-unit` but PASSES
  3/3 in isolation (`go test ./internal/protocred/`). Pure in-memory test (no env/files);
  unrelated to the cluster-key-PSK-protection change (which doesn't touch protocred).
  Likely shared-global/parallel-binary pollution. Surfaced 2026-05-30.

- [ ] **Cluster4Node HeadObject가 expired object에 `MethodNotAllowed` 반환 [P1]**:
  `tests/e2e/lifecycle_expiration_test.go:111-118` TagFilter spec에서 expire 후
  HeadObject가 S3 표준 `NotFound` 대신 `MethodNotAllowed`를 반환. 기존 testify
  `assert.Equal` soft-fail이 가렸던 동작 — Ginkgo gomega.Expect는 hard fail이라
  노출됨. 현재 Or(NotFound, MethodNotAllowed) matcher로 마스킹. 원인 후보:
  `internal/cluster/backend.go` `deleteObjectWithMarker` 가 non-versioned bucket
  expiration 후에도 delete-marker semantics 적용. S3-parity 위해 NotFound 일관화
  필요.

- [ ] **Cluster tests create QUIC transports without closing them [P2]**:
  ~60 call sites in `internal/cluster/*_test.go` create
  `transport.MustNewQUICTransport(...)` but never `Close()` it, so quic-go's
  `Transport.listen` / `sendQueue.Run` / `Conn.run` goroutines live until process
  exit. They accumulate across the cluster test binary and are the goroutines that
  `ec_fix_integration_test.go`'s process-global `goleak.VerifyNone` was catching
  (now scoped via `goleak.IgnoreCurrent` baseline). Hygiene cleanup: add a
  `newTestTransport(t)` helper that registers `t.Cleanup(tr.Close)` and migrate the
  call sites. Mechanical but large; not required for correctness.

- [ ] **`TestBlobStoreAppendNoCompressKeepsAllocationBound` race-mode fail [P2]**:
  baseline에서도 동일하게 fail (allocations=4 vs ≤1 expected). `-race` 빌드에서
  추가 alloc churn. packblob 패키지 별도 작업.

- [ ] **Reverify full `make test-e2e` baseline [P1]**: old snapshot listed
  backup_restic, multipart (`capability multipart_listing_v1 rejected`),
  cluster_incident, quarantine_incident, dynamic_join_services,
  cluster_scrubber, no_peers, encryption_at_rest, iam_scoped_key. Since the
  capability evidence endpoint now exists, rerun the baseline and split true
  remaining failures from stale bootstrap/warmup artifacts.

## Conformance Follow-Ups

- [ ] [nfs-conformance] pynfs-nightly [P1]: run pynfs basic suite on a scheduled
  Linux/Colima host and review `results/summary.json`.
- [ ] [nfs-conformance] nfstest-runner [P2]: add nfstest after pynfs stabilizes.
## Storage And Volume Backlog

- [ ] **Volume pool quota operator surface**: `volume.ManagerOptions.PoolQuota`
  and planner/unit coverage exist; expose/configure the cross-volume physical
  capacity pool through the server/CLI path before treating it as an operator
  feature.
- [ ] **Multi-tenancy**: account/namespace isolation above IAM.
- [ ] **Quota**: capacity limits per service account or team.
- [ ] **Volume export/import**: backup/restore for volume data.
- [ ] **Volume policy**: per-volume pool quota, encryption key, and EC
  profile overrides.
- [ ] **Volume attach/detach**: runtime NBD exposure toggles.
- [ ] **Volume rename**: migrate volume block key prefixes.

> Volume dedup, snapshot, clone, rollback, and copy-on-write were removed in
> v0.0.346.0 (plain block device). Items above that previously assumed those
> features are descoped accordingly and depend on the eventual volume redesign.
- [ ] **`__grainfs_volumes` EC policy**: revisit forced replication for volume
  blocks after EC scrub and migration strategy are ready.

## Migration Backlog

- [ ] NFS virtual overlay.
- [ ] NBD block proxying.
- [ ] Bucket-level server-side injection:
  - Phase 1 credentials shipped in v0.0.123.0.
  - Phase 2 import hook shipped in v0.0.171.0.
  - Remaining work: mirror mode, cutover verb, progress tracking, upstream
    `List`/`Head`/`CopyObject`, CLI integration, cancellation on leadership loss,
    and counter continuity after leader flip.

## Parked

- [ ] **Redesign disaster-recovery surface.** `grainfs recover` and `grainfs doctor` were removed in v0.0.343.0 (shipped partial/misleading). Before reintroducing: (1) define failure domains explicitly (rack/AZ/region); (2) name commands by what they actually do (`recover metadata` vs `recover data`, never bare `recover cluster`); (3) `doctor` must do real integrity checks, not directory-existence stubs; (4) RPO + data-vs-metadata semantics must be in help text. Until then the `recover` verb stays unbound.
- [ ] Redis protocol.
- [ ] TSDB.
- [ ] 9P/NFS shared write-back layer.
- [ ] Blame Mode v2.
- [ ] PagerDuty native webhook mapping.
- [ ] go-billy Direct File I/O / O_DIRECT.
- [ ] Hot/cold auto tiering.
- [ ] I/O-based auto rebalancing.
- [ ] io_uring.
- [ ] SPDK.
- [ ] SIMD.
- [ ] SoA layout.
- [ ] Control-plane/data-plane split.
- [ ] fix(storage/packblob): extend versioning bypass to Suspended state (currently only Enabled bypasses fast path; Suspended buckets on single-node still pack-write under (bucket,key) without versionId="null"). Add e2e cases for Suspended → PUT/DELETE/HEAD by versionId.
- [ ] feat(scrubber): multi-node/multi-group segment GC fan-out. Orphan-segment GC currently (Plan 3.5) activates only on group-0's distBackend AND, in a cluster, runs only on the raft leader (CaughtUp uses node.ReadIndex → followers get ErrNotLeader → fail-closed skip). Result: single-node is complete; in a multi-node cluster, segments on non-leader nodes' local disks and in non-group-0 data-groups are never reclaimed → latent disk growth. Proper design needs leader-coordinated (or per-node-with-freshness-barrier) deletion across all groups — mirror the EC scrub ecResolver fan-out (boot_phases_scrubber.go) and decide who deletes follower-local raw segments. SegmentOrphanLog already namespaces by groupID. Blocked-by: Plan 3.5 (object-segment-gc-activation) land.
- [ ] [P3] refactor(server/iceberg): collapse the duplicated `routePathOAuthTokenSuffix = "v1/oauth/tokens"` constant. After the iceberg sub-package extraction the literal lives in BOTH core `route_paths.go` (consumed by the route-surface manifest in `route_surface.go`) and `internal/server/iceberg/iceberg_routes.go` (consumed by `Handler.Register`). Two sources of truth: if one is edited without the other, core's skip-S3-authz route-surface marker points to a different path than where the handler actually registers the OAuth endpoint → silent auth-surface drift. Behavior-neutral today (both literals identical, self-documented in both files). Fix: have core pass the suffix into `Handler.Register(hz, suffix, prefixes...)`, or export one canonical const. Surfaced by the iceberg-subpackage extraction code gate.
- [ ] [P3] refactor(server): extract alerts → `internal/server/alertssvc` (descoped from the satellites batch that shipped incidentsvc + snapshotsvc). The PRODUCTION extraction is clean/mechanical (same closure-Deps one-way pattern as iceberg/receipt/incident/snapshot; `AlertsState` type + handlers move, core keeps a `*alertssvc.AlertsState` staging field + calls exported `AddOnStateChange`/`Close`, serveruntime rewires `server.NewAlertsStateWithConfig`→`alertssvc.NewTracking...`). It was descoped ONLY because the alerts TEST harness is deeply coupled to package-server test infrastructure: `alerts_api_test.go`'s `startTestAlertsServer` constructs `&Server{alerts:st}`, reaches `AlertsState` internals (`st.dispatcher.Start/DrainForTest/Stop`), uses core's `localhostOnly()` middleware, and depends on shared helpers `waitForTCP`/`shutdownTestServer`/`testServerShutdownTimeout`/`serverTestTB`/`serverCleanupTB` (defined in `server_test.go`). Relocating to `package alertssvc` requires forking that interface/helper chain — bloats the diff and risks test drift. Do this with: either (a) extract the shared server-test infra to a small importable test-support package first, then move the alerts tests + rewrite the harness to `NewHandler`+`Register` with a pass-through `LocalhostOnly` Dep + hardcoded route paths; or (b) rewrite the alerts HTTP tests as true black-box against an assembled `server.New(...)` (no `st.dispatcher` reach-in). Production surface compiler-verified cycle-0. Surfaced by the satellites-batch extraction (2026-05-30).

## Completed

