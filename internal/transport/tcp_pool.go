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
}

func newConnPool(capPerPeer int, idleTimeout time.Duration) *connPool {
	return &connPool{
		cap:         capPerPeer,
		idleTimeout: idleTimeout,
		idle:        make(map[string][]idleConn),
		total:       make(map[string]int),
		waiters:     make(map[string][]chan net.Conn),
	}
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

// popFreshIdleLocked returns the newest non-expired idle conn for addr, appending
// any evicted (expired) conns to *victims for the caller to close after unlocking.
func (t *connPool) popFreshIdleLocked(addr string, victims *[]net.Conn) net.Conn {
	q := t.idle[addr]
	for len(q) > 0 {
		ic := q[len(q)-1]
		q = q[:len(q)-1]
		if t.idleTimeout > 0 && time.Since(ic.lastUsed) >= t.idleTimeout {
			*victims = append(*victims, ic.c) // presumed server-reaped → evict
			if t.cap > 0 {
				t.total[addr]--
			}
			continue
		}
		t.idle[addr] = q
		return ic.c
	}
	t.idle[addr] = q
	return nil
}

// checkin returns a clean conn: hand it to a waiter, else add it to the idle list.
func (t *connPool) checkin(addr string, c net.Conn) {
	t.mu.Lock()
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
	_ = c.Close()
	t.dialFailed(addr)
}

// dialFailed releases a counted-but-unused dial slot (a dial error, a discard, or
// a canceled grant) and hands the next queued waiter a fresh dial slot if any.
func (t *connPool) dialFailed(addr string) {
	t.mu.Lock()
	if t.cap > 0 {
		t.total[addr]--
	}
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
	t.waiters[addr] = q[1:]
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
			t.waiters[addr] = append(q[:i], q[i+1:]...)
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
		}
		delete(t.idle, addr)
	}
	t.total = make(map[string]int)
	t.mu.Unlock()
	closeConns(victims)
}

func closeConns(cs []net.Conn) {
	for _, c := range cs {
		_ = c.Close()
	}
}
