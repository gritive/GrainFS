package transport

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestControlPool_NotStarvedByBulk proves the control-plane forward (CallPooled)
// is served from a SEPARATE per-peer pool, so a flood of bulk shard streams that
// exhausts the (tiny) bulk pool cap cannot starve the short control RPC — the
// root cause of CompleteMultipartUpload 500s under concurrent large-object load.
//
// Setup: bulk cap = 2; two concurrent CallWithBody calls occupy both bulk slots
// and HANG (the server body handler blocks). On the shared-pool (pre-fix) code a
// third checkout — CallPooled — blocks on the exhausted cap until its ctx
// deadline (RED). With a separate control pool it returns promptly (GREEN).
func TestControlPool_NotStarvedByBulk(t *testing.T) {
	const psk = "control-lane"
	srv := startTCP(t, psk)
	release := make(chan struct{})
	entered := make(chan struct{}, 2)
	srv.HandleBody(StreamShardWriteBody, func(req *Message, body io.Reader) *Message {
		entered <- struct{}{}
		<-release // hold the conn (and its bulk pool slot) until released
		_, _ = io.Copy(io.Discard, body)
		return NewResponse(req, nil)
	})
	srv.Handle(StreamAdmin, func(req *Message) *Message {
		return NewResponse(req, []byte("pong"))
	})

	cli := MustNewTCPTransport(psk)
	cli.setConfigForTest(TCPTransportConfig{MaxConnsPerPeer: 2}) // tiny BULK cap
	t.Cleanup(func() { _ = cli.Close() })
	addr := srv.LocalAddr()

	// Occupy both bulk slots with hanging CallWithBody calls.
	var wg sync.WaitGroup
	bulkCtx, bulkCancel := context.WithCancel(context.Background())
	defer bulkCancel()
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = cli.CallWithBody(bulkCtx, addr, &Message{Type: StreamShardWriteBody}, bytes.NewReader([]byte("x")))
		}()
	}
	// Wait until both bulk slots are actually held server-side.
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("bulk calls did not occupy both slots")
		}
	}

	// Control forward must NOT be starved by the saturated bulk pool.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	resp, err := cli.CallPooled(ctx, addr, &Message{Type: StreamAdmin, Payload: []byte("ping")})
	require.NoError(t, err, "control forward starved behind bulk (shared pool)")
	require.Equal(t, "pong", string(resp.Payload))
	require.Less(t, time.Since(start), 1*time.Second,
		"control forward should return promptly, not wait for the bulk pool to free")

	close(release)
	wg.Wait()
}

// TestControlPool_RecycledOnIdentityRotation proves the SECURITY-critical invariant:
// the separate control pool is recycled by RecycleConns exactly like the bulk pool.
// Both halves of the S5a gen-guard must hold for controlPool — idle conns drained
// (closeAll) AND an in-flight conn discarded on checkin under the bumped generation.
// Without wiring controlPool into RecycleConns, a control conn handshaked under a
// now-dropped/revoked identity would survive and be reused (regression).
func TestControlPool_RecycledOnIdentityRotation(t *testing.T) {
	srv := startTCP(t, "control-recycle-key")
	cli := MustNewTCPTransport("control-recycle-key")
	t.Cleanup(func() { _ = cli.Close() })
	addr := srv.LocalAddr()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Check out + stamp a control conn held across the recycle (the gen-guard case).
	// Done FIRST, while the pool is empty, so it dials fresh (not reuse the idle one).
	inflight, reused, err := cli.getControlConn(ctx, addr)
	require.NoError(t, err)
	require.False(t, reused)

	// Park a separate idle conn in the CONTROL pool (the closeAll-drain case).
	idleConn, err := cli.dial(ctx, addr)
	require.NoError(t, err)
	cli.controlPool.checkin(addr, idleConn)
	require.Equal(t, 1, controlPoolLen(cli, addr), "one idle control conn pooled")

	cli.RecycleConns() // must bump controlPool gen AND drain its idle conns

	assert.Equal(t, 0, controlPoolLen(cli, addr), "control pool must be drained on RecycleConns")
	_ = idleConn.SetReadDeadline(time.Now().Add(time.Second))
	_, rerr := idleConn.Read(make([]byte, 1))
	assert.Error(t, rerr, "the recycled idle control conn must be closed")

	// In-flight conn must be DISCARDED on checkin under the bumped gen, not re-pooled.
	cli.controlPool.checkin(addr, inflight)
	assert.Equal(t, 0, controlPoolLen(cli, addr),
		"a control conn checked out before RecycleConns must be discarded on checkin, not re-pooled")
}

// TestControlPool_ClosePeerDropsControlPool is the per-peer analogue: ClosePeer must
// recycle the control pool for the revoked peer (idle drain + in-flight gen-guard).
func TestControlPool_ClosePeerDropsControlPool(t *testing.T) {
	srv := startTCP(t, "control-closepeer-key")
	cli := MustNewTCPTransport("control-closepeer-key")
	t.Cleanup(func() { _ = cli.Close() })
	addr := srv.LocalAddr()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	inflight, _, err := cli.getControlConn(ctx, addr)
	require.NoError(t, err)

	idleConn, err := cli.dial(ctx, addr)
	require.NoError(t, err)
	cli.controlPool.checkin(addr, idleConn)
	require.Equal(t, 1, controlPoolLen(cli, addr))

	cli.ClosePeer(addr) // must bump this peer's controlPool gen + drain its idle conns

	assert.Equal(t, 0, controlPoolLen(cli, addr), "ClosePeer must drain the control pool for the peer")
	_ = idleConn.SetReadDeadline(time.Now().Add(time.Second))
	_, rerr := idleConn.Read(make([]byte, 1))
	assert.Error(t, rerr, "ClosePeer must close the peer's pooled control conn")

	cli.controlPool.checkin(addr, inflight)
	assert.Equal(t, 0, controlPoolLen(cli, addr),
		"a control conn checked out before ClosePeer must be discarded on checkin")
}
