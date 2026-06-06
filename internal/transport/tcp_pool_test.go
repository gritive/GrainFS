package transport

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// poolLen returns the idle-conn count for addr in the bulk data-plane pool
// (test-only introspection).
func poolLen(t *TCPTransport, addr string) int {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	return len(t.pool.idle[addr])
}

// controlPoolLen returns the idle-conn count for addr in the control-plane pool
// (CallPooled lives here, separate from the bulk pool).
func controlPoolLen(t *TCPTransport, addr string) int {
	t.controlPool.mu.Lock()
	defer t.controlPool.mu.Unlock()
	return len(t.controlPool.idle[addr])
}

func TestTCPPool_ReuseAcrossSequentialTransfers(t *testing.T) {
	const psk = "pool-reuse"
	srv := startTCP(t, psk)
	srv.HandleBody(StreamShardWriteBody, func(req *Message, body io.Reader) *Message {
		got, _ := io.ReadAll(body)
		return NewResponse(req, []byte(fmt.Sprintf("n=%d", len(got))))
	})
	cli := MustNewTCPTransport(psk)
	t.Cleanup(func() { _ = cli.Close() })
	addr := srv.LocalAddr()

	for i := 0; i < 4; i++ {
		// SHORT per-call deadline + an idle gap that EXCEEDS it before the next
		// checkout. If the clean path failed to clear the deadline before checkin,
		// transfer i+1 would check out a conn whose deadline is already in the past
		// and fail its first I/O — so this gap is what makes the test a real guard
		// against the stale-deadline bug (not a pass-by-timing-luck).
		ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
		payload := make([]byte, 64<<10)
		_, _ = rand.Read(payload)
		resp, err := cli.CallWithBody(ctx, addr, &Message{Type: StreamShardWriteBody}, bytes.NewReader(payload))
		cancel()
		require.NoErrorf(t, err, "transfer %d (a stale checked-in deadline would fail here)", i)
		assert.Equal(t, fmt.Sprintf("n=%d", len(payload)), string(resp.Payload))
		time.Sleep(500 * time.Millisecond) // exceed the 400ms per-call deadline
	}
	// After clean sequential transfers the pool should hold exactly 1 reused conn.
	assert.Equal(t, 1, poolLen(cli, addr), "clean transfers must reuse one conn")
}

func TestTCPPool_CallPooledReusesConn(t *testing.T) {
	const psk = "callpooled-reuse"
	srv := startTCP(t, psk)
	srv.Handle(StreamAdmin, func(req *Message) *Message {
		return NewResponse(req, append([]byte("ok:"), req.Payload...))
	})
	cli := MustNewTCPTransport(psk)
	t.Cleanup(func() { _ = cli.Close() })
	addr := srv.LocalAddr()

	// CallPooled is the bodyless hot-path RPC (ShardService.SendRequest leader
	// forward). Like the CallWithBody reuse guard above, a SHORT per-call deadline
	// plus an idle gap that exceeds it proves the clean path clears the deadline
	// before checkin — a connection-per-RPC implementation (the pre-fix Call) would
	// instead leave poolLen at 0.
	for i := 0; i < 4; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
		resp, err := cli.CallPooled(ctx, addr, &Message{Type: StreamAdmin, Payload: []byte(fmt.Sprintf("r%d", i))})
		cancel()
		require.NoErrorf(t, err, "CallPooled transfer %d", i)
		assert.Equal(t, fmt.Sprintf("ok:r%d", i), string(resp.Payload))
		time.Sleep(500 * time.Millisecond) // exceed the 400ms per-call deadline
	}
	assert.Equal(t, 1, controlPoolLen(cli, addr), "clean CallPooled transfers must reuse one conn in the control pool")
}

func TestTCPPool_ErroredTransferKeepsCleanConn(t *testing.T) {
	const psk = "pool-err"
	srv := startTCP(t, psk)
	srv.HandleBody(StreamShardWriteBody, func(req *Message, body io.Reader) *Message {
		_, _ = io.ReadAll(body)
		return NewErrorResponse(req, StatusError, assertErr{})
	})
	cli := MustNewTCPTransport(psk)
	t.Cleanup(func() { _ = cli.Close() })
	addr := srv.LocalAddr()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cli.CallWithBody(ctx, addr, &Message{Type: StreamShardWriteBody}, bytes.NewReader([]byte("x")))
	require.Error(t, err) // StatusError
	// A StatusError frame is a CLEAN cycle (frame fully read, no body) → reusable.
	assert.Equal(t, 1, poolLen(cli, addr))
}

func TestTCPPool_CancelledTransferDiscardsConn(t *testing.T) {
	const psk = "pool-cancel"
	srv := startTCP(t, psk)
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	srv.HandleBody(StreamShardWriteBody, func(req *Message, body io.Reader) *Message {
		_, _ = io.ReadAll(body)
		<-block // never responds → client ctx will cancel
		return NewResponse(req, nil)
	})
	cli := MustNewTCPTransport(psk)
	t.Cleanup(func() { _ = cli.Close() })
	addr := srv.LocalAddr()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := cli.CallWithBody(ctx, addr, &Message{Type: StreamShardWriteBody}, bytes.NewReader([]byte("x")))
	require.Error(t, err) // ctx deadline → conn closed mid-transfer
	assert.Equal(t, 0, poolLen(cli, addr), "cancelled transfer must NOT return its conn to the pool")
}

func TestTCPPool_CallReadEarlyCloseDiscardsConn(t *testing.T) {
	const psk = "pool-read-early"
	srv := startTCP(t, psk)
	big := make([]byte, 3<<20)
	_, _ = rand.Read(big)
	srv.HandleRead(StreamShardReadBody, func(req *Message) (*Message, io.ReadCloser) {
		return NewResponse(req, []byte("meta")), io.NopCloser(bytes.NewReader(big))
	})
	cli := MustNewTCPTransport(psk)
	t.Cleanup(func() { _ = cli.Close() })
	addr := srv.LocalAddr()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, rc, err := cli.CallRead(ctx, addr, &Message{Type: StreamShardReadBody})
	require.NoError(t, err)
	// Read only a little, then Close BEFORE the terminator → conn must be discarded.
	buf := make([]byte, 1024)
	_, _ = rc.Read(buf)
	require.NoError(t, rc.Close())
	assert.Equal(t, 0, poolLen(cli, addr), "early-closed CallRead must NOT pool its conn")

	// A FULLY-drained CallRead, by contrast, returns its conn.
	_, rc2, err := cli.CallRead(ctx, addr, &Message{Type: StreamShardReadBody})
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, rc2)
	require.NoError(t, rc2.Close())
	assert.Equal(t, 1, poolLen(cli, addr), "drained CallRead returns its conn")
}

// TestConnPool_EmptyKeysReclaimedAfterPeerDrains guards the line-101 residual: the
// idle/total/waiters maps must not retain a per-peer entry forever. After a peer's
// conns fully drain, none of the three maps should hold a key for that addr (an
// unbounded leak over process lifetime under churning membership otherwise).
func TestConnPool_EmptyKeysReclaimedAfterPeerDrains(t *testing.T) {
	p := newConnPool(4, time.Minute)
	addr := "10.0.0.1:9000"
	a, b := net.Pipe()
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })

	// Grant a dial slot (saturated-from-empty), stamp + check the conn in (total=1).
	conn, err := p.checkout(context.Background(), addr)
	require.NoError(t, err)
	require.Nil(t, conn, "first checkout grants a dial slot")
	p.stampWith(a, p.genSnapshot(addr))
	p.checkin(addr, a)

	// Drain: check the idle conn out, then discard it (frees the counted slot).
	got, err := p.checkout(context.Background(), addr)
	require.NoError(t, err)
	require.Equal(t, a, got)
	p.discard(addr, got)

	p.mu.Lock()
	defer p.mu.Unlock()
	_, hasIdle := p.idle[addr]
	_, hasTotal := p.total[addr]
	_, hasWaiters := p.waiters[addr]
	require.False(t, hasIdle, "idle map key must be reclaimed after the peer drains")
	require.False(t, hasTotal, "total map key must be reclaimed after the peer drains")
	require.False(t, hasWaiters, "waiters map key must never linger")
}

// TestConnPool_WaiterBackingArrayReleasedOnDrain guards the second half of line 101:
// popWaiterLocked advanced the queue via waiters[addr]=q[1:], which keeps the popped
// channels pinned in the original backing array (slice-aliasing retention) and never
// removes the key. Each pop must release its slot, and the key must be reclaimed once
// the queue drains.
func TestConnPool_WaiterBackingArrayReleasedOnDrain(t *testing.T) {
	p := newConnPool(1, time.Minute)
	addr := "10.0.0.2:9000"
	w1 := make(chan net.Conn, 1)
	w2 := make(chan net.Conn, 1)
	w3 := make(chan net.Conn, 1)

	p.mu.Lock()
	p.waiters[addr] = []chan net.Conn{w1, w2, w3}
	backing := p.waiters[addr] // alias the original backing array
	require.Equal(t, w1, p.popWaiterLocked(addr))
	require.Equal(t, w2, p.popWaiterLocked(addr))
	require.Equal(t, w3, p.popWaiterLocked(addr))
	_, has := p.waiters[addr]
	p.mu.Unlock()

	require.False(t, has, "waiters key must be reclaimed once the queue drains")
	require.Nil(t, backing[0], "popped waiter[0] must be released from the backing array")
	require.Nil(t, backing[1], "popped waiter[1] must be released from the backing array")
	require.Nil(t, backing[2], "popped waiter[2] must be released from the backing array")
}

// TestConnPool_RemoveWaiterCompactsWithoutAliasing guards removeWaiter's hygiene: a
// canceled waiter is dequeued by in-place compaction that must (a) preserve FIFO order,
// (b) clear the freed tail slot so the removed channel isn't pinned in the backing
// array, and (c) reclaim the key once the queue drains.
func TestConnPool_RemoveWaiterCompactsWithoutAliasing(t *testing.T) {
	p := newConnPool(1, time.Minute)
	addr := "10.0.0.3:9000"
	w1 := make(chan net.Conn, 1)
	w2 := make(chan net.Conn, 1)
	w3 := make(chan net.Conn, 1)
	p.mu.Lock()
	p.waiters[addr] = []chan net.Conn{w1, w2, w3}
	backing := p.waiters[addr]
	p.mu.Unlock()

	p.removeWaiter(addr, w2) // dequeue the middle waiter (not yet sender-committed)

	p.mu.Lock()
	q := p.waiters[addr]
	p.mu.Unlock()
	require.Equal(t, []chan net.Conn{w1, w3}, q, "removeWaiter must compact preserving FIFO order")
	require.Nil(t, backing[2], "the freed tail slot must be cleared (no aliasing retention)")

	// Drain the rest → the key is reclaimed.
	p.removeWaiter(addr, w1)
	p.removeWaiter(addr, w3)
	p.mu.Lock()
	_, has := p.waiters[addr]
	p.mu.Unlock()
	require.False(t, has, "waiters key reclaimed once all waiters are removed")
}

func TestTCPPool_ConcurrentTransfersGetExclusiveConns(t *testing.T) {
	const psk = "pool-conc"
	srv := startTCP(t, psk)
	srv.HandleBody(StreamShardWriteBody, func(req *Message, body io.Reader) *Message {
		got, _ := io.ReadAll(body)
		return NewResponse(req, got) // echo the body back
	})
	cli := MustNewTCPTransport(psk)
	t.Cleanup(func() { _ = cli.Close() })
	addr := srv.LocalAddr()

	const n = 16
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			payload := bytes.Repeat([]byte{byte(i)}, 128<<10)
			resp, err := cli.CallWithBody(ctx, addr, &Message{Type: StreamShardWriteBody}, bytes.NewReader(payload))
			if err != nil {
				errs[i] = err
				return
			}
			if !bytes.Equal(resp.Payload, payload) {
				errs[i] = fmt.Errorf("transfer %d body corrupted (conn not exclusive)", i)
			}
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		assert.NoErrorf(t, err, "transfer %d", i)
	}
}
