package storage

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage/eccodec"
	"github.com/stretchr/testify/require"
)

// Encoding an EC shard through the real DEKKeeper adapter while a DEK rotation
// runs concurrently must never fail and must always round-trip: chunks 1+ pin
// chunk 0's gen even if Rotate() advances the active gen mid-encode, and old
// gens stay resident so decode at the pinned gen succeeds. This is the S4
// concurrency smoke test (the deterministic guard is eccodec's
// *PinsGenAcrossMidStreamDrift); meaningful under `-race`.
func TestEncodeEncryptedShard_ConcurrentRotation(t *testing.T) {
	kek := bytes.Repeat([]byte{0x22}, encrypt.KEKSize)
	keeper, err := encrypt.NewDEKKeeper(kek, testSeamClusterID())
	require.NoError(t, err)
	adapter := NewDEKKeeperAdapter(keeper, testSeamClusterID())

	fields := []encrypt.AADField{encrypt.FieldString("b"), encrypt.FieldString("k"), encrypt.FieldUint32(3)}
	// Several chunks per shard so a rotation has a window between chunk seals.
	plain := bytes.Repeat([]byte("racy-shard-payload"), eccodec.DefaultEncryptedChunkSize/4)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			select {
			case <-stop:
				return
			default:
				_ = keeper.Rotate() // old gens stay resident (no prune)
				time.Sleep(20 * time.Microsecond)
			}
		}
	}()

	for i := 0; i < 100; i++ {
		var buf bytes.Buffer
		require.NoError(t, eccodec.EncodeEncryptedShard(&buf, bytes.NewReader(plain), adapter, fields, eccodec.DefaultEncryptedChunkSize),
			"encode must not fail under concurrent rotation")
		var out bytes.Buffer
		require.NoError(t, eccodec.DecodeEncryptedShard(&out, bytes.NewReader(buf.Bytes()), adapter, fields))
		require.Equal(t, plain, out.Bytes(), "round-trip mismatch under concurrent rotation")
	}
	close(stop)
	wg.Wait()
}
