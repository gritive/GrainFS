package transport

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- test-only pool seams ---------------------------------------------------

type fakeConn struct {
	mu     sync.Mutex
	closed bool
}

func (f *fakeConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (f *fakeConn) Write(b []byte) (int, error)      { return len(b), nil }
func (f *fakeConn) LocalAddr() net.Addr              { return fakeAddr{} }
func (f *fakeConn) RemoteAddr() net.Addr             { return fakeAddr{} }
func (f *fakeConn) SetDeadline(time.Time) error      { return nil }
func (f *fakeConn) SetReadDeadline(time.Time) error  { return nil }
func (f *fakeConn) SetWriteDeadline(time.Time) error { return nil }
func (f *fakeConn) Close() error {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
	return nil
}
func (f *fakeConn) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

type fakeAddr struct{}

func (fakeAddr) Network() string { return "fake" }
func (fakeAddr) String() string  { return "fake" }

func (p *connPool) idleLen(addr string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.idle[addr])
}

func (p *connPool) totalForTest(addr string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.total[addr]
}

func (p *connPool) checkinAtForTest(addr string, c net.Conn, at time.Time) {
	p.mu.Lock()
	p.idle[addr] = append(p.idle[addr], idleConn{c: c, lastUsed: at})
	if p.cap > 0 {
		p.total[addr]++
	}
	p.mu.Unlock()
}

// mustCheckout returns a conn plus whether a fresh dial slot was granted, dialing
// a fakeConn when so (the slot is already counted by checkout).
func mustCheckout(t *testing.T, p *connPool, addr string) (net.Conn, bool) {
	t.Helper()
	c, err := p.checkout(context.Background(), addr)
	require.NoError(t, err)
	if c != nil {
		return c, false // reused
	}
	return &fakeConn{}, true
}

// --- pool unit tests --------------------------------------------------------

func TestTCPPool_CapBlocksUntilCheckin(t *testing.T) {
	p := newConnPool(1, time.Minute)
	addr := "10.0.0.1:1"
	c1, dial1 := mustCheckout(t, p, addr)
	require.True(t, dial1, "first checkout under cap must grant a dial slot")

	done := make(chan struct{})
	go func() { _, _ = p.checkout(context.Background(), addr); close(done) }()
	select {
	case <-done:
		t.Fatal("second checkout must block while at cap")
	case <-time.After(100 * time.Millisecond):
	}
	p.checkin(addr, c1) // hands the conn to the waiter
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("checkin must wake the waiter")
	}
}

func TestTCPPool_CheckoutCtxCancelDequeues(t *testing.T) {
	p := newConnPool(1, time.Minute)
	addr := "10.0.0.2:1"
	c1, _ := mustCheckout(t, p, addr)
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { _, err := p.checkout(ctx, addr); errc <- err }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	require.ErrorIs(t, <-errc, context.Canceled)
	// The cancelled waiter must be removed: a later checkin returns to idle, not to a dead waiter.
	p.checkin(addr, c1)
	assert.Equal(t, 1, p.idleLen(addr))
}

func TestTCPPool_EvictsIdleExpired(t *testing.T) {
	p := newConnPool(4, 50*time.Millisecond)
	addr := "10.0.0.3:1"
	fc := &fakeConn{}
	p.checkinAtForTest(addr, fc, time.Now().Add(-time.Second)) // already-expired idle
	c, err := p.checkout(context.Background(), addr)
	require.NoError(t, err)
	assert.Nil(t, c, "expired idle must be evicted, forcing a fresh dial slot")
	assert.True(t, fc.isClosed(), "evicted idle conn must be closed")
	assert.Equal(t, 0, p.idleLen(addr))
}

// TestTCPPool_RemoveWaiterAwaitsLateCommit is the advisor BLOCKER guard. It forces
// the interleaving where a sender pops the waiter and commits its send AFTER
// removeWaiter has begun: the fixed (blocking) removeWaiter must receive the late
// commit and reclaim it; the old select/default version took default and leaked.
func TestTCPPool_RemoveWaiterAwaitsLateCommit(t *testing.T) {
	p := newConnPool(1, time.Minute)
	addr := "10.0.0.4:1"
	ch := make(chan net.Conn, 1) // a popped waiter channel (NOT in the waiters list)
	p.mu.Lock()
	p.total[addr] = 1
	p.mu.Unlock()

	done := make(chan struct{})
	go func() { p.removeWaiter(addr, ch); close(done) }()
	time.Sleep(20 * time.Millisecond) // ensure removeWaiter is past dequeue, now waiting
	fc := &fakeConn{}
	ch <- fc // late commit (the stall window a buggy select/default would miss)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("removeWaiter did not consume the late-committed conn (leak)")
	}
	assert.Equal(t, 1, p.idleLen(addr), "late-committed conn must be reclaimed to idle, not leaked")
	assert.False(t, fc.isClosed(), "reclaimed conn returns to idle (not closed)")
}

func TestTCPPool_RemoveWaiterAwaitsLateDialSlot(t *testing.T) {
	p := newConnPool(1, time.Minute)
	addr := "10.0.0.5:1"
	ch := make(chan net.Conn, 1)
	p.mu.Lock()
	p.total[addr] = 1
	p.mu.Unlock()

	done := make(chan struct{})
	go func() { p.removeWaiter(addr, ch); close(done) }()
	time.Sleep(20 * time.Millisecond)
	ch <- nil // late "dial slot granted" commit
	<-done
	assert.Equal(t, 0, p.totalForTest(addr), "granted-but-unused dial slot must be released")
}
