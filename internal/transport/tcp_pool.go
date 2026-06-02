package transport

import (
	"context"
	"net"
	"sync"
	"time"
)

// idleConn is a checked-in conn plus the time it went idle (for eviction).
type idleConn struct {
	c        net.Conn
	lastUsed time.Time
}

// connPool is a bounded, queued per-peer data-plane conn pool (S3b). It extends
// S3a's unbounded idle free-list:
//
//   - cap bounds TOTAL conns per peer (in-flight + idle). cap == 0 = unlimited
//     (S3a behavior: every checkout grants a fresh dial slot, idle is unbounded).
//   - When saturated, checkout blocks in a FIFO waiter queue until a conn frees.
//   - Idle conns older than idleTimeout are evicted on checkout (presumed
//     server-reaped/half-open, since the server idle-reaps at a matching bound).
//
// Invariant: a conn is "counted" in total from the moment a dial slot is granted
// (checkout returns nil,nil) until it is discarded. checkin/dialFailed hand a
// freed conn or dial slot directly to the next waiter so the slot never leaks.
type connPool struct {
	mu          sync.Mutex
	cap         int // max total (in-flight + idle) per peer; 0 = unlimited
	idleTimeout time.Duration
	idle        map[string][]idleConn
	total       map[string]int             // in-flight + idle per peer (cap>0 only)
	waiters     map[string][]chan net.Conn // FIFO; a nil conn == "dial slot granted"

	// Identity-generation guard (S5a). Every dialed conn is stamped with the
	// (global, per-peer) generation at dial. RecycleConns bumps the global gen and
	// ClosePeer bumps a peer's gen; checkin then DISCARDS (never re-pools) any conn
	// stamped below the current gen, so a conn handshaked under a now-dropped/revoked
	// identity that was in-flight during the recycle cannot be reused afterwards.
	// (closeAll/closePeer already drain IDLE conns; this closes the in-flight hole.)
	globalGen uint64
	peerGen   map[string]uint64
	connGen   map[net.Conn]connStamp
}

type connStamp struct{ global, peer uint64 }

func newConnPool(capPerPeer int, idleTimeout time.Duration) *connPool {
	return &connPool{
		cap:         capPerPeer,
		idleTimeout: idleTimeout,
		idle:        make(map[string][]idleConn),
		total:       make(map[string]int),
		waiters:     make(map[string][]chan net.Conn),
		peerGen:     make(map[string]uint64),
		connGen:     make(map[net.Conn]connStamp),
	}
}

// genSnapshot reads the current (global, per-peer) generation. The dialer captures
// this BEFORE dialing so a recycle/revoke that fires DURING the handshake stamps
// the conn with the pre-dial generation (→ discarded on checkin). Capturing after
// the handshake would record the post-bump generation and let the conn — whose TLS
// handshake ran under the now-stale identity — be re-pooled.
func (t *connPool) genSnapshot(addr string) connStamp {
	t.mu.Lock()
	defer t.mu.Unlock()
	return connStamp{global: t.globalGen, peer: t.peerGen[addr]}
}

// stampWith records a conn's identity generation (captured pre-dial via
// genSnapshot). Pooled conns keep their original stamp across checkout/checkin.
func (t *connPool) stampWith(c net.Conn, cs connStamp) {
	t.mu.Lock()
	t.connGen[c] = cs
	t.mu.Unlock()
}

// bumpGlobalGen invalidates every existing conn (cluster-wide recycle).
func (t *connPool) bumpGlobalGen() {
	t.mu.Lock()
	t.globalGen++
	t.mu.Unlock()
}

// bumpPeerGen invalidates every existing conn to one peer (per-peer revoke).
func (t *connPool) bumpPeerGen(addr string) {
	t.mu.Lock()
	t.peerGen[addr]++
	t.mu.Unlock()
}

// staleLocked reports whether c's stamp predates the current generation for addr.
// Caller holds t.mu.
func (t *connPool) staleLocked(addr string, c net.Conn) bool {
	cs, ok := t.connGen[c]
	return ok && (cs.global < t.globalGen || cs.peer < t.peerGen[addr])
}

// checkout returns a pooled conn, or (nil, nil) meaning "dial slot granted — the
// caller must dial; the slot is already counted." It blocks when the peer is at
// cap until a conn frees or ctx ends.
func (t *connPool) checkout(ctx context.Context, addr string) (net.Conn, error) {
	t.mu.Lock()
	var victims []net.Conn
	if c := t.popFreshIdleLocked(addr, &victims); c != nil {
		t.mu.Unlock()
		closeConns(victims)
		return c, nil
	}
	if t.cap == 0 || t.total[addr] < t.cap {
		if t.cap > 0 {
			t.total[addr]++ // count the slot now; dial happens outside the lock (cap==0 never reads total)
		}
		t.mu.Unlock()
		closeConns(victims)
		return nil, nil
	}
	// Saturated: enqueue and wait.
	ch := make(chan net.Conn, 1)
	t.waiters[addr] = append(t.waiters[addr], ch)
	t.mu.Unlock()
	closeConns(victims)
	select {
	case c := <-ch:
		if c == nil { // a freed slot was granted → caller dials (slot already counted)
			return nil, nil
		}
		return c, nil
	case <-ctx.Done():
		t.removeWaiter(addr, ch) // dequeue; reclaim if a sender already committed
		return nil, ctx.Err()
	}
}

// popFreshIdleLocked returns the newest idle conn for addr that is neither expired
// nor identity-stale, appending any evicted conns to *victims for the caller to
// close after unlocking.
func (t *connPool) popFreshIdleLocked(addr string, victims *[]net.Conn) net.Conn {
	q := t.idle[addr]
	var found net.Conn
	for len(q) > 0 {
		ic := q[len(q)-1]
		q[len(q)-1] = idleConn{} // release the popped entry from the backing array
		q = q[:len(q)-1]
		// Evict an idle conn that is expired (presumed server-reaped) OR stamped
		// below the current identity generation. The staleness check makes the
		// recycle guard order-independent: RecycleConns' bumpGlobalGen and closeAll
		// are separate lock acquisitions, so a checkout racing between them could
		// otherwise pop a not-yet-drained stale idle conn and reuse it for one
		// transfer under the dropped identity. Rejecting it here (in addition to the
		// checkin guard) closes that read-side window.
		expired := t.idleTimeout > 0 && time.Since(ic.lastUsed) >= t.idleTimeout
		if expired || t.staleLocked(addr, ic.c) {
			*victims = append(*victims, ic.c)
			delete(t.connGen, ic.c)
			t.decTotalLocked(addr)
			continue
		}
		found = ic.c
		break
	}
	t.storeIdleLocked(addr, q)
	return found
}

// storeIdleLocked writes back a peer's idle queue, deleting the map key when the queue
// is empty so a fully-drained peer leaves no lingering entry (and the empty slice's
// backing array becomes collectable). Caller holds t.mu.
func (t *connPool) storeIdleLocked(addr string, q []idleConn) {
	if len(q) == 0 {
		delete(t.idle, addr)
		return
	}
	t.idle[addr] = q
}

// decTotalLocked decrements a peer's counted-conn total (cap>0 only) and deletes the
// key once it reaches zero, so a drained peer leaves no lingering total entry. Caller
// holds t.mu.
func (t *connPool) decTotalLocked(addr string) {
	if t.cap == 0 {
		return
	}
	t.total[addr]--
	if t.total[addr] == 0 {
		delete(t.total, addr)
	}
}

// checkin returns a clean conn: hand it to a waiter, else add it to the idle list.
// A conn whose identity generation is stale (a recycle/revoke fired while it was
// in-flight) is DISCARDED instead of reused — the security-critical invariant.
func (t *connPool) checkin(addr string, c net.Conn) {
	t.mu.Lock()
	if t.staleLocked(addr, c) {
		t.mu.Unlock()
		t.discard(addr, c) // stale identity → close + free slot, never reuse
		return
	}
	if w := t.popWaiterLocked(addr); w != nil {
		t.mu.Unlock()
		w <- c // stays counted; the waiter uses it directly
		return
	}
	t.idle[addr] = append(t.idle[addr], idleConn{c: c, lastUsed: time.Now()})
	t.mu.Unlock()
}

// discard closes a dirty/dead conn and frees its slot (waking a waiter).
func (t *connPool) discard(addr string, c net.Conn) {
	t.mu.Lock()
	delete(t.connGen, c)
	t.mu.Unlock()
	_ = c.Close()
	t.dialFailed(addr)
}

// dialFailed releases a counted-but-unused dial slot (a dial error, a discard, or
// a canceled grant) and hands the next queued waiter a fresh dial slot if any.
func (t *connPool) dialFailed(addr string) {
	t.mu.Lock()
	t.decTotalLocked(addr)
	if w := t.popWaiterLocked(addr); w != nil {
		if t.cap > 0 {
			t.total[addr]++ // reassign the freed slot to the waiter's dial
		}
		t.mu.Unlock()
		w <- nil // "dial slot granted"
		return
	}
	t.mu.Unlock()
}

func (t *connPool) popWaiterLocked(addr string) chan net.Conn {
	q := t.waiters[addr]
	if len(q) == 0 {
		return nil
	}
	w := q[0]
	q[0] = nil // release the popped channel so the q[1:] advance can't pin it alive
	q = q[1:]
	if len(q) == 0 {
		delete(t.waiters, addr) // drained → reclaim the key and the backing array
	} else {
		t.waiters[addr] = q
	}
	return w
}

// removeWaiter dequeues a canceled checkout's channel. If the channel is no longer
// in the list, a sender (checkin/dialFailed) has ALREADY popped it and is committed
// to an unconditional, lock-free send — so we MUST block to receive it and
// redistribute, otherwise the delivered conn (or dial slot) leaks forever and, at
// cap, permanently shrinks the peer's pool. (The send buffer is size 1 and only one
// sender can target a popped channel, so the receive is guaranteed and deadlock-free
// because senders never hold mu during the send.)
func (t *connPool) removeWaiter(addr string, ch chan net.Conn) {
	t.mu.Lock()
	q := t.waiters[addr]
	for i, w := range q {
		if w == ch {
			n := copy(q[i:], q[i+1:])
			q[i+n] = nil // clear the freed tail slot (no aliasing retention)
			q = q[:len(q)-1]
			if len(q) == 0 {
				delete(t.waiters, addr)
			} else {
				t.waiters[addr] = q
			}
			t.mu.Unlock()
			return // dequeued before any sender committed
		}
	}
	t.mu.Unlock()
	// A sender committed before we could dequeue. Block to receive and redistribute
	// via checkin/dialFailed so nothing leaks and any other waiter is woken.
	if c := <-ch; c != nil {
		t.checkin(addr, c)
	} else {
		t.dialFailed(addr)
	}
}

func (t *connPool) closeAll() {
	t.mu.Lock()
	victims := make([]net.Conn, 0)
	for addr, q := range t.idle {
		for _, ic := range q {
			victims = append(victims, ic.c)
			delete(t.connGen, ic.c)
			// Decrement per idle conn (keep total accurate; do NOT reset to a fresh map,
			// or a still-in-flight conn's later discard would drive total negative and
			// bypass the cap). decTotalLocked deletes the key once it reaches zero.
			t.decTotalLocked(addr)
		}
		delete(t.idle, addr)
	}
	t.mu.Unlock()
	closeConns(victims)
}

// closePeer closes the idle pooled conns for one peer (S5a ClosePeer / per-peer
// recycle). Like closeAll, it leaves in-flight conns to their owner's discard but
// keeps total accurate so the cap is not bypassed.
func (t *connPool) closePeer(addr string) {
	t.mu.Lock()
	victims := make([]net.Conn, 0, len(t.idle[addr]))
	for _, ic := range t.idle[addr] {
		victims = append(victims, ic.c)
		delete(t.connGen, ic.c)
		t.decTotalLocked(addr)
	}
	delete(t.idle, addr)
	t.mu.Unlock()
	closeConns(victims)
}

func closeConns(cs []net.Conn) {
	for _, c := range cs {
		_ = c.Close()
	}
}
