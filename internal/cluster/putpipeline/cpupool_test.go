package putpipeline

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage/eccodec"
	"github.com/stretchr/testify/require"
)

func TestCPUPool_RegisterAndDispatch_OneShardPerChannel(t *testing.T) {
	in := make(chan StripePlaintext, 4)
	pool := &CPUPool{
		in:      in,
		enc:     testShardEncryptor(t),
		ecCfg:   cluster.ECConfig{DataShards: 2, ParityShards: 2},
		workers: 2,
	}
	shardChans := make([]chan EncryptedShardChunk, 4)
	for i := range shardChans {
		shardChans[i] = make(chan EncryptedShardChunk, 4)
	}
	sends := make([]chan<- EncryptedShardChunk, 4)
	for i := range shardChans {
		sends[i] = shardChans[i]
	}
	pool.registerPut(1, "testbucket", "testkey", 1<<20, sends)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pool.Run(ctx)

	in <- StripePlaintext{
		PutID:     1,
		StripeIdx: 0,
		Data:      make([]byte, 1<<20),
		LastInPut: true,
	}

	for i := 0; i < 4; i++ {
		select {
		case chunk := <-shardChans[i]:
			require.Equal(t, i, chunk.ShardIdx)
			require.Equal(t, uint64(1), chunk.PutID)
			require.True(t, chunk.LastInPut)
		case <-time.After(2 * time.Second):
			t.Fatalf("shard %d did not receive a chunk", i)
		}
	}
}

func TestCPUPool_OrderedPerShard_FiveStripes(t *testing.T) {
	const stripe = 1 << 20
	in := make(chan StripePlaintext, 8)
	pool := &CPUPool{
		in:      in,
		enc:     testShardEncryptor(t),
		ecCfg:   cluster.ECConfig{DataShards: 2, ParityShards: 2},
		workers: 4,
	}
	shardChans := make([]chan EncryptedShardChunk, 4)
	sends := make([]chan<- EncryptedShardChunk, 4)
	for i := range shardChans {
		shardChans[i] = make(chan EncryptedShardChunk, 8)
		sends[i] = shardChans[i]
	}
	pool.registerPut(7, "testbucket", "testkey", 5<<20, sends)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pool.Run(ctx)

	for i := 0; i < 5; i++ {
		in <- StripePlaintext{
			PutID:     7,
			StripeIdx: uint32(i),
			Data:      make([]byte, stripe),
			LastInPut: i == 4,
		}
	}
	for shard := 0; shard < 4; shard++ {
		for want := uint32(0); want < 5; want++ {
			select {
			case chunk := <-shardChans[shard]:
				require.Equal(t, want, chunk.StripeIdx, "shard %d expected stripe %d", shard, want)
			case <-time.After(2 * time.Second):
				t.Fatalf("shard %d did not receive stripe %d", shard, want)
			}
		}
	}
}

func TestCPUPool_ConcatenatedShardIsValidGFSENC3(t *testing.T) {
	const stripe = 1 << 20
	shardEnc := testShardEncryptor(t)
	in := make(chan StripePlaintext, 8)
	pool := &CPUPool{
		in:      in,
		enc:     shardEnc,
		ecCfg:   cluster.ECConfig{DataShards: 2, ParityShards: 2},
		workers: 4,
	}
	shardChans := make([]chan EncryptedShardChunk, 4)
	sends := make([]chan<- EncryptedShardChunk, 4)
	for i := range shardChans {
		shardChans[i] = make(chan EncryptedShardChunk, 16)
		sends[i] = shardChans[i]
	}
	pool.registerPut(11, "testbucket", "testobj/v1", 3<<20, sends)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pool.Run(ctx)

	// Three stripes so the shard file spans multiple stripes — the exact
	// case the per-stripe-writer bug corrupted.
	const stripes = 3
	for i := 0; i < stripes; i++ {
		data := make([]byte, stripe)
		for j := range data {
			data[j] = byte((i*7 + j) % 251)
		}
		in <- StripePlaintext{
			PutID:     11,
			StripeIdx: uint32(i),
			Data:      data,
			LastInPut: i == stripes-1,
		}
	}

	// For each shard, concatenate every chunk's ciphertext in stripe
	// order, then confirm the result is a single decodable GFSENC3
	// stream under that shard's base fields.
	for shard := 0; shard < 4; shard++ {
		var concatenated []byte
		for got := 0; got < stripes; got++ {
			select {
			case chunk := <-shardChans[shard]:
				require.Equal(t, uint32(got), chunk.StripeIdx)
				require.NoError(t, chunk.Err, "shard %d stripe %d unexpected error", shard, got)
				concatenated = append(concatenated, chunk.Ciphertext...)
			case <-time.After(2 * time.Second):
				t.Fatalf("shard %d missing chunk for stripe %d", shard, got)
			}
		}
		var plain bytes.Buffer
		err := eccodec.DecodeEncryptedShard(&plain, bytes.NewReader(concatenated), shardEnc, cluster.ShardAADFields("testbucket", "testobj/v1", shard))
		require.NoError(t, err, "shard %d: concatenated chunks are not one valid GFSENC3 stream", shard)
		require.NotZero(t, plain.Len(), "shard %d decoded to empty", shard)
	}
}

// fakeGenDriftEncryptor simulates a ShardEncryptor that increments gen each
// call, triggering the gen-pinning check in EncryptedShardChunkedWriter.
type fakeGenDriftEncryptor struct {
	enc *encrypt.Encryptor
	gen uint32
}

func (f *fakeGenDriftEncryptor) Seal(domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte) ([]byte, uint32, error) {
	aad := encrypt.BuildAAD(domain, make([]byte, 16), fields...)
	ct, err := f.enc.SealValueAADTo(nil, aad, plain)
	if err != nil {
		return nil, 0, err
	}
	g := f.gen
	f.gen++
	return ct, g, nil
}

func (f *fakeGenDriftEncryptor) Open(domain encrypt.AADDomain, fields []encrypt.AADField, _ uint32, ct []byte) ([]byte, error) {
	aad := encrypt.BuildAAD(domain, make([]byte, 16), fields...)
	return f.enc.OpenValueAADTo(nil, aad, ct)
}

// TestCPUPool_SealError_PropagatesToCommitCh drives a PUT through the pool with
// a gen-drifting encryptor (fails on the 2nd chunk via gen-pinning) and wires
// the per-shard channels into real DriveActors so the error-chunk flows through
// DriveActor.handle's chunk.Err short-circuit into commitCh. It verifies the
// plan's Task 5 Step 4 contract:
//
//	(1) exactly one ShardWriteResult per shard reaches commitCh,
//	(2) the failed shards' results carry Err != nil,
//	(3) no leftover shard_N final file for any failed shard (tmp cleaned).
func TestCPUPool_SealError_PropagatesToCommitCh(t *testing.T) {
	// 2 MiB stripes ⟹ each data shard gets 1 MiB/stripe; with a 1 MiB chunk
	// size, the first stripe seals chunk 0 (gen 0, opens the drive tmp) and the
	// second stripe trips the gen-pin on chunk 1 — exercising the failChunk
	// (pending-state) branch of the drive short-circuit.
	const stripe = 2 << 20
	enc, err := encrypt.NewEncryptor(make([]byte, 32))
	require.NoError(t, err)
	driftEnc := &fakeGenDriftEncryptor{enc: enc}

	tmp := t.TempDir()
	const numShards = 4
	in := make(chan StripePlaintext, 4)
	pool := &CPUPool{
		in:      in,
		enc:     driftEnc,
		ecCfg:   cluster.ECConfig{DataShards: 2, ParityShards: 2},
		workers: 2,
	}

	// One DriveActor per shard, all reporting into a shared commitCh, mirroring
	// the Pipeline wiring (round-robin shard→drive, here 1:1).
	commit := make(chan ShardWriteResult, numShards*4)
	drives := make([]*DriveActor, numShards)
	driveIns := make([]chan EncryptedShardChunk, numShards)
	shardChans := make([]chan<- EncryptedShardChunk, numShards)
	for i := 0; i < numShards; i++ {
		driveIns[i] = make(chan EncryptedShardChunk, 16)
		shardChans[i] = driveIns[i]
		drives[i] = &DriveActor{
			in:       driveIns[i],
			dataDir:  tmp,
			commitCh: commit,
			pending:  make(map[uint64]*shardWriteState),
		}
		drives[i].registerPut(42, "b", "k", i)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pool.Run(ctx)
	for i := range drives {
		go drives[i].Run(ctx)
	}

	pool.registerPut(42, "b", "k", 2<<20, shardChans)

	for i := 0; i < 2; i++ {
		in <- StripePlaintext{
			PutID:     42,
			StripeIdx: uint32(i),
			Data:      make([]byte, stripe),
			LastInPut: i == 1,
		}
	}

	// Collect exactly one ShardWriteResult per shard from commitCh.
	results := make(map[int]ShardWriteResult)
	deadline := time.After(5 * time.Second)
	for len(results) < numShards {
		select {
		case res := <-commit:
			require.Equal(t, uint64(42), res.PutID)
			if _, dup := results[res.ShardIdx]; dup {
				t.Fatalf("shard %d: duplicate ShardWriteResult on commitCh", res.ShardIdx)
			}
			results[res.ShardIdx] = res
		case <-deadline:
			t.Fatalf("only got %d/%d shard results before timeout", len(results), numShards)
		}
	}

	// The gen-drift encryptor fails every shard's 2nd chunk, so every shard's
	// result must carry an error and no final shard_N file may survive.
	failedAtLeastOne := false
	for shard := 0; shard < numShards; shard++ {
		res := results[shard]
		if res.Err != nil {
			failedAtLeastOne = true
		}
		require.Error(t, res.Err, "shard %d: gen-drift seal failure must surface as ShardWriteResult.Err", shard)
		_, statErr := os.Stat(filepath.Join(tmp, "b", "k", fmt.Sprintf("shard_%d", shard)))
		require.True(t, os.IsNotExist(statErr), "shard %d: failed shard must not leave a final shard_%d file", shard, shard)
	}
	require.True(t, failedAtLeastOne, "expected at least one shard seal failure")

	// No straggler results after every shard reported once.
	select {
	case res := <-commit:
		t.Fatalf("unexpected extra ShardWriteResult: shard=%d err=%v", res.ShardIdx, res.Err)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestDriveActor_ErrChunk_FailChunkBranch covers DriveActor.handle's
// chunk.Err short-circuit when an in-progress tmp file already exists
// (pending state non-nil): a successful chunk opens the tmp, then an
// error-chunk arrives → failChunk closes+unlinks the tmp and emits ONE
// failed ShardWriteResult.
func TestDriveActor_ErrChunk_FailChunkBranch(t *testing.T) {
	tmp := t.TempDir()
	in := make(chan EncryptedShardChunk, 4)
	commit := make(chan ShardWriteResult, 4)
	d := &DriveActor{in: in, dataDir: tmp, commitCh: commit, pending: make(map[uint64]*shardWriteState)}
	d.registerPut(1, "bucket", "key", 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.Run(ctx)

	// First chunk opens the tmp (pending state established), no terminal result yet.
	in <- EncryptedShardChunk{PutID: 1, ShardIdx: 0, Ciphertext: []byte("partial"), LastInPut: false}
	// Error-chunk for the same shard ⟹ failChunk branch (pending != nil).
	in <- EncryptedShardChunk{PutID: 1, ShardIdx: 0, LastInPut: true, Err: fmt.Errorf("simulated seal failure")}

	select {
	case res := <-commit:
		require.Equal(t, uint64(1), res.PutID)
		require.Equal(t, 0, res.ShardIdx)
		require.Error(t, res.Err)
	case <-time.After(2 * time.Second):
		t.Fatal("no commit result for error-chunk (failChunk branch)")
	}
	// tmp must be cleaned and no final file created.
	_, err := os.Stat(filepath.Join(tmp, "bucket", "key", "shard_0.tmp"))
	require.True(t, os.IsNotExist(err), "failChunk must unlink the tmp file")
	_, err = os.Stat(filepath.Join(tmp, "bucket", "key", "shard_0"))
	require.True(t, os.IsNotExist(err), "no final shard file must be created")

	// Exactly one result: no straggler.
	select {
	case res := <-commit:
		t.Fatalf("unexpected second result: %+v", res)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestDriveActor_ErrChunk_DirectEmitBranch covers DriveActor.handle's
// chunk.Err short-circuit when NO tmp was ever opened (pending state nil,
// e.g. seal failed on the shard's first stripe): the actor must drop the
// registry entry and emit ONE failed ShardWriteResult without touching disk.
func TestDriveActor_ErrChunk_DirectEmitBranch(t *testing.T) {
	tmp := t.TempDir()
	in := make(chan EncryptedShardChunk, 4)
	commit := make(chan ShardWriteResult, 4)
	d := &DriveActor{in: in, dataDir: tmp, commitCh: commit, pending: make(map[uint64]*shardWriteState)}
	d.registerPut(1, "bucket", "key", 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.Run(ctx)

	// Error-chunk as the FIRST chunk for the shard ⟹ pending == nil ⟹ direct-emit branch.
	in <- EncryptedShardChunk{PutID: 1, ShardIdx: 0, LastInPut: true, Err: fmt.Errorf("first-stripe seal failure")}

	select {
	case res := <-commit:
		require.Equal(t, uint64(1), res.PutID)
		require.Equal(t, 0, res.ShardIdx)
		require.Error(t, res.Err)
	case <-time.After(2 * time.Second):
		t.Fatal("no commit result for error-chunk (direct-emit branch)")
	}
	// No file should ever be created.
	_, err := os.Stat(filepath.Join(tmp, "bucket", "key", "shard_0.tmp"))
	require.True(t, os.IsNotExist(err), "no tmp file must be created in the direct-emit branch")
	_, err = os.Stat(filepath.Join(tmp, "bucket", "key", "shard_0"))
	require.True(t, os.IsNotExist(err), "no final shard file must be created")

	// The registry entry must be dropped: a subsequent chunk for the same PutID
	// is treated as an unregistered put (proving dropPending ran).
	in <- EncryptedShardChunk{PutID: 1, ShardIdx: 0, Ciphertext: []byte("late"), LastInPut: true}
	select {
	case res := <-commit:
		require.Error(t, res.Err, "post-drop chunk must report unregistered put")
	case <-time.After(2 * time.Second):
		t.Fatal("no result for post-drop chunk")
	}
}
