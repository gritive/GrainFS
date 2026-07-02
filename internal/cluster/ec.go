package cluster

// Phase 18 Cluster EC: production EC helpers shared by PutObject/GetObject.
// Intra-group shard placement uses weighted Rendezvous Hashing (see
// selectShardPlacement / internal/hrw); the writer records the chosen NodeIDs in
// segment metadata and readers replay them, so placement is computed exactly
// once per write.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/gritive/GrainFS/internal/metrics"
)

const (
	// MaxAutoDataShards caps zero-config data shard fan-out. Parity remains
	// fixed at two for normal clusters; larger durability policies belong in a
	// future expert/archival policy, not the startup CLI.
	MaxAutoDataShards = 6
	AutoParityShards  = 2
)

// ECConfig controls cluster erasure coding behavior.
type ECConfig struct {
	// DataShards (k) — number of data shards (e.g. 4 for 4+2).
	DataShards int
	// ParityShards (m) — number of parity shards (e.g. 2 for 4+2).
	ParityShards int
}

// NumShards returns k+m.
func (c ECConfig) NumShards() int { return c.DataShards + c.ParityShards }

// IsActive returns true when cfg can place every shard on the supplied node set.
// A 1+0 profile is active on one node: it uses the EC pipeline without
// redundancy, which keeps cluster object storage on one code path.
func (c ECConfig) IsActive(clusterSize int) bool {
	return c.DataShards > 0 && c.ParityShards >= 0 && c.NumShards() > 0 && clusterSize >= c.NumShards()
}

// AutoECConfigForClusterSize returns the zero-config EC profile for a cluster
// size. Operators do not provide k/m at startup; node count selects the profile.
func AutoECConfigForClusterSize(nodes int) ECConfig {
	switch {
	case nodes <= 0:
		return ECConfig{}
	case nodes == 1:
		return ECConfig{DataShards: 1, ParityShards: 0}
	case nodes == 2:
		return ECConfig{DataShards: 1, ParityShards: 1}
	case nodes == 3:
		return ECConfig{DataShards: 2, ParityShards: 1}
	default:
		data := nodes - AutoParityShards
		if data > MaxAutoDataShards {
			data = MaxAutoDataShards
		}
		return ECConfig{DataShards: data, ParityShards: AutoParityShards}
	}
}

func (c ECConfig) Redundant() bool {
	return c.ParityShards > 0
}

// EffectiveConfig returns target when it fits the supplied node set. Startup
// passes an auto-selected target; tests may still inject explicit configs with
// SetECConfig to exercise low-level EC behavior.
func EffectiveConfig(n int, target ECConfig) ECConfig {
	if !target.IsActive(n) {
		return ECConfig{}
	}
	return target
}

// shardHeaderSize is the per-shard prefix that records original object size
// so Reconstruct can call reedsolomon.Join(writer, shards, dataLen).
const shardHeaderSize = 8

func encodeShardHeader(origSize int64) [shardHeaderSize]byte {
	var h [shardHeaderSize]byte
	binary.BigEndian.PutUint64(h[:], uint64(origSize))
	return h
}

func decodeShardHeader(data []byte) (origSize int64, body []byte, err error) {
	if len(data) < shardHeaderSize {
		return 0, nil, fmt.Errorf("shard too small for header: %d bytes", len(data))
	}
	size := int64(binary.BigEndian.Uint64(data[:shardHeaderSize]))
	return size, data[shardHeaderSize:], nil
}

// ECSplit encodes object data into k+m shards ready for per-node storage.
// Each returned shard already contains the size header. Result length == cfg.NumShards().
func ECSplit(cfg ECConfig, data []byte) ([][]byte, error) {
	if len(data) == 0 {
		header := encodeShardHeader(0)
		out := make([][]byte, cfg.NumShards())
		for i := range out {
			payload := make([]byte, shardHeaderSize)
			copy(payload, header[:])
			out[i] = payload
		}
		return out, nil
	}
	shards, err := ecSplitBodies(cfg, data)
	if err != nil {
		return nil, err
	}
	header := encodeShardHeader(int64(len(data)))
	out := make([][]byte, len(shards))
	for i, s := range shards {
		payload := make([]byte, shardHeaderSize+len(s))
		copy(payload, header[:])
		copy(payload[shardHeaderSize:], s)
		out[i] = payload
	}
	return out, nil
}

func ecSplitBodies(cfg ECConfig, data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return make([][]byte, cfg.NumShards()), nil
	}
	enc, err := getEncoder(cfg)
	if err != nil {
		return nil, fmt.Errorf("ec encoder: %w", err)
	}
	shards, err := enc.Split(data)
	if err != nil {
		return nil, fmt.Errorf("ec split: %w", err)
	}
	if err := enc.Encode(shards); err != nil {
		return nil, fmt.Errorf("ec encode: %w", err)
	}
	return shards, nil
}

func ecSplitBodiesPooled(cfg ECConfig, data []byte) ([][]byte, [][]byte, error) {
	n := cfg.NumShards()
	if len(data) == 0 {
		return make([][]byte, n), nil, nil
	}
	enc, err := getEncoder(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("ec encoder: %w", err)
	}
	perShard := ecSplitBackingSize(cfg, len(data)) / n
	fullShards := len(data) / perShard
	paddingCount := n - fullShards
	padding := getECSplitPaddingShards(paddingCount, perShard)

	shards := make([][]byte, n)
	remaining := data
	for i := 0; i < fullShards; i++ {
		shards[i] = remaining[:perShard:perShard]
		remaining = remaining[perShard:]
	}
	for i := 0; i < paddingCount; i++ {
		pad := padding[i]
		if fullShards+i < cfg.DataShards {
			clear(pad)
		}
		if i == 0 && len(remaining) > 0 {
			copy(pad, remaining)
		}
		shards[fullShards+i] = pad
	}
	if err := enc.Encode(shards); err != nil {
		putECSplitPaddingShards(padding)
		return nil, nil, fmt.Errorf("ec encode: %w", err)
	}
	return shards, padding, nil
}

func ecSplitBackingSize(cfg ECConfig, dataLen int) int {
	if dataLen <= 0 || cfg.DataShards <= 0 {
		return 0
	}
	return ((dataLen + cfg.DataShards - 1) / cfg.DataShards) * cfg.NumShards()
}

// ecSplitRawInto is the allocation-reusing variant of ecSplitBodies: instead
// of letting reedsolomon.Split allocate a fresh shard matrix per call (~1 MiB
// for a 1 MiB stripe — the largest single PUT-path allocation), it lays the
// data + parity shards out in the caller-supplied dst backing and computes
// parity via the cached encoder's Encode. The returned shards alias dst, so
// the caller must keep dst alive until every shard has been consumed, then may
// recycle it. The returned []byte is dst's (possibly reallocated) backing for
// the caller to pool. Output is byte-identical to ECSplitRaw — see
// TestECSplitRawInto_ByteIdenticalToSplit.
func ecSplitRawInto(cfg ECConfig, data []byte, dst []byte) (shards [][]byte, backing []byte, err error) {
	n := cfg.NumShards()
	if len(data) == 0 {
		return make([][]byte, n), dst, nil
	}
	enc, err := getEncoder(cfg)
	if err != nil {
		return nil, dst, fmt.Errorf("ec encoder: %w", err)
	}
	perShard := ecSplitBackingSize(cfg, len(data)) / n
	total := perShard * n
	if cap(dst) < total {
		dst = make([]byte, total)
	} else {
		dst = dst[:total]
	}
	copy(dst, data)
	clear(dst[len(data):total]) // zero the data-shard padding tail + parity region
	shards = make([][]byte, n)
	for i := 0; i < n; i++ {
		shards[i] = dst[i*perShard : (i+1)*perShard : (i+1)*perShard]
	}
	if err := enc.Encode(shards); err != nil {
		return nil, dst, fmt.Errorf("ec encode: %w", err)
	}
	return shards, dst, nil
}

// ECSplitRawInto is the exported wrapper over ecSplitRawInto. The returned
// shards alias the returned backing slice; the caller pools the backing and
// must not recycle it until every shard has been consumed. dst may be nil (a
// fresh backing is allocated).
func ECSplitRawInto(cfg ECConfig, data []byte, dst []byte) (shards [][]byte, backing []byte, err error) {
	return ecSplitRawInto(cfg, data, dst)
}

// ECSplitRaw is the header-less variant: returns k+m shard byte slices
// WITHOUT the 8-byte size header, for callers that write the size header
// once per shard file (not per stripe).
func ECSplitRaw(cfg ECConfig, data []byte) ([][]byte, error) {
	return ecSplitBodies(cfg, data)
}

// ShardHeader returns the 8-byte big-endian length header that prefixes
// every shard's plaintext stream. CPUPool writes this once per shard at
// PUT start so the reader recovers the exact body size regardless of
// how many stripes the writer fed through.
func ShardHeader(origSize int64) [shardHeaderSize]byte {
	return encodeShardHeader(origSize)
}

// ECReconstruct assembles the original data from at least k of k+m shards.
// Missing shards are represented by nil entries. Returns the original bytes.
func ECReconstruct(cfg ECConfig, shards [][]byte) ([]byte, error) {
	origSize, bodies, err := ecReconstructBodies(cfg, shards)
	if err != nil {
		return nil, err
	}
	if origSize == 0 {
		return []byte{}, nil
	}
	var buf writeBuffer
	buf.b = make([]byte, 0, origSize)
	if err := ecReconstructBodiesTo(&buf, cfg, origSize, bodies); err != nil {
		return nil, err
	}
	return buf.b, nil
}

// ECReconstructTo assembles the original data from at least k of k+m shards and
// writes it to w without allocating a full output buffer.
func ECReconstructTo(w io.Writer, cfg ECConfig, shards [][]byte) error {
	origSize, bodies, err := ecReconstructBodies(cfg, shards)
	if err != nil {
		return err
	}
	if origSize == 0 {
		return nil
	}
	return ecReconstructBodiesTo(w, cfg, origSize, bodies)
}

// ECReconstructStreamTo assembles the original data from shard streams and
// writes it to w without holding full shard bodies in memory. Missing shards
// are represented by nil readers.
func ECReconstructStreamTo(w io.Writer, cfg ECConfig, shards []io.Reader) error {
	// No metadata anchor at this call site: fall back to legacy first-shard-seed
	// consensus.
	origSize, bodies, err := ecReconstructStreamBodies(cfg, shards, -1)
	if err != nil {
		return err
	}
	if origSize == 0 {
		return nil
	}
	bodies = ecWrapBodiesExactLen(cfg, origSize, bodies, nil, nil)
	return ecReconstructStreamBodiesTo(w, cfg, origSize, bodies)
}

// newECReconstructStreamReaderWithPrefetch builds a streaming EC reader. closeShards,
// if non-nil, releases the underlying shard readers/bodies; it is invoked when the
// returned reader is closed. For both background-producer paths (prefetch and the
// missing-data pipe branch) it runs only AFTER the producers have exited (they must
// not be mid-Read on a shard body when it is closed — Hertz forbids cross-goroutine
// CloseBodyStream; see internal/transport/http_shared.go), and it is detached so
// Close returns promptly instead of blocking on a stalled producer.
// Every shard body is bounded to its exact expected length: a shard that ends
// cleanly short surfaces *ecShardTruncatedError (reported to onShardFault, if
// non-nil, with the shard index) instead of mis-splicing the next shard's bytes.
// onShardClean, if non-nil, fires once per shard when its body is delivered in
// full with no fault — clean-completion healthy evidence for the serving peer.
// objectSize, when >= 0, anchors shard-header validation to the
// metadata-authoritative size (see ecReconstructStreamBodies); pass -1 for the
// legacy first-shard-seed consensus mode.
func newECReconstructStreamReaderWithPrefetch(cfg ECConfig, shards []io.Reader, objectSize int64, closeShards func(), onShardFault, onShardClean func(int)) (*ecReconstructStreamReader, error) {
	origSize, bodies, err := ecReconstructStreamBodies(cfg, shards, objectSize)
	if err != nil {
		return nil, err
	}
	bodies = ecWrapBodiesExactLen(cfg, origSize, bodies, onShardFault, onShardClean)
	if ecStreamHasAllDataShards(cfg, bodies) {
		dataReaders, err := ecReconstructStreamDataReaders(cfg, bodies)
		if err != nil {
			return nil, err
		}
		// Single-shard reads (e.g., 1+0 single-node EC) get no parallelism
		// benefit from the async prefetcher and the extra goroutine hop only
		// adds latency, so stay on the direct io.MultiReader path. Reading
		// happens in the consumer goroutine, so closing shards on Close is safe
		// synchronously (no concurrent producer).
		if len(dataReaders) <= 1 {
			return &ecReconstructStreamReader{
				reader: io.LimitReader(io.MultiReader(dataReaders...), origSize),
				close:  syncShardClose(closeShards),
			}, nil
		}
		prefetchers := make([]*asyncPrefetchReader, len(dataReaders))
		readers := make([]io.Reader, len(dataReaders))
		for i, dr := range dataReaders {
			p := newAsyncPrefetchReader(dr, nil)
			prefetchers[i] = p
			readers[i] = p
		}
		closeAll := func() error {
			// Stop the producers without blocking. signalStop cannot interrupt a
			// Read already in flight (S8-2 forbids cross-goroutine
			// CloseBodyStream), so a producer stalled mid-Read on a remote shard
			// still returns from that one Read before it notices — but
			// readFullOrStop's stop-check between Reads means a trickling peer no
			// longer re-arms the wait forever; see readFullOrStop. Wait for them
			// to exit and close the shard bodies in a detached goroutine so Close
			// returns promptly while preserving the invariant that bodies are
			// closed only after producers stop reading.
			for _, p := range prefetchers {
				p.signalStop()
			}
			metrics.ECDetachedTeardowns.Inc()
			go func() {
				defer metrics.ECDetachedTeardowns.Dec()
				for _, p := range prefetchers {
					p.awaitDrained()
				}
				if closeShards != nil {
					closeShards()
				}
			}()
			return nil
		}
		return &ecReconstructStreamReader{
			reader: io.LimitReader(io.MultiReader(readers...), origSize),
			close:  closeAll,
		}, nil
	}
	pr, pw := io.Pipe()
	// stop lets the reconstruct goroutine notice an abort BETWEEN reads: it
	// cannot interrupt an in-flight Read (S8-2 forbids cross-goroutine
	// CloseBodyStream), but a trickling peer's next progressing Read now leads
	// to a stop-check exit instead of re-arming the idle deadline forever; see
	// readFullOrStop.
	stop := make(chan struct{})
	// done gates the detached shard teardown: bodies may be closed only after
	// the reconstruct goroutine has exited (it reads them via readFullOrStop —
	// Hertz forbids cross-goroutine CloseBodyStream; see
	// internal/transport/http_shared.go). pr.Close() does NOT interrupt an
	// in-flight shard read — it only fails the goroutine's next pw.Write — so
	// after an abort the shard bodies (and their connections) are retained
	// until the current in-flight Read returns: against a trickling peer that
	// is the next progressing Read (bounded by readFullOrStop's stop-check),
	// against a fully stalled peer it remains the idle read timeout.
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := ecReconstructMissingDataStreamTo(pw, cfg, origSize, bodies, stop); err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		_ = pw.Close()
	}()
	return &ecReconstructStreamReader{reader: pr, close: func() error {
		err := pr.Close()
		close(stop)
		if closeShards != nil {
			metrics.ECDetachedTeardowns.Inc()
			go func() {
				defer metrics.ECDetachedTeardowns.Dec()
				<-done
				closeShards()
			}()
		}
		return err
	}}, nil
}

// syncShardClose adapts a closeShards callback to the reader's close signature,
// invoking it synchronously (used where no background producer reads the shards).
func syncShardClose(closeShards func()) func() error {
	if closeShards == nil {
		return nil
	}
	return func() error {
		closeShards()
		return nil
	}
}

type ecReconstructStreamReader struct {
	reader io.Reader
	close  func() error
	// closed is atomic so a concurrent double-Close is a safe no-op: the
	// teardown funcs close channels (close(stop)) and must run exactly once.
	closed atomic.Bool
}

func (r *ecReconstructStreamReader) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

func (r *ecReconstructStreamReader) Close() error {
	if !r.closed.CompareAndSwap(false, true) {
		return nil
	}
	if r.close != nil {
		return r.close()
	}
	return nil
}

func ecStreamHasAllDataShards(cfg ECConfig, bodies []io.Reader) bool {
	if len(bodies) < cfg.DataShards {
		return false
	}
	for i := 0; i < cfg.DataShards; i++ {
		if bodies[i] == nil {
			return false
		}
	}
	return true
}

var ecDataShardBufferPool sync.Pool

func getECDataShardBuffer(size int) []byte {
	if v := ecDataShardBufferPool.Get(); v != nil {
		bufp := v.(*[]byte)
		if cap(*bufp) >= size {
			return (*bufp)[:size]
		}
	}
	return make([]byte, size)
}

func putECDataShardBuffer(buf []byte) {
	if buf != nil {
		b := buf[:cap(buf)]
		ecDataShardBufferPool.Put(&b)
	}
}

func ecReconstructBodiesTo(w io.Writer, cfg ECConfig, origSize int64, bodies [][]byte) error {
	enc, err := getEncoder(cfg)
	if err != nil {
		return fmt.Errorf("ec decoder: %w", err)
	}
	if err := enc.ReconstructData(bodies); err != nil {
		return fmt.Errorf("ec reconstruct: %w", err)
	}
	if err := enc.Join(w, bodies, int(origSize)); err != nil {
		return fmt.Errorf("ec join: %w", err)
	}
	return nil
}

func ecReconstructStreamBodiesTo(w io.Writer, cfg ECConfig, origSize int64, bodies []io.Reader) error {
	dataReaders, err := ecReconstructStreamDataReaders(cfg, bodies)
	if err != nil {
		return err
	}
	if dataReaders != nil {
		if _, err := io.CopyN(w, io.MultiReader(dataReaders...), origSize); err != nil {
			return fmt.Errorf("ec join: %w", err)
		}
		return nil
	}
	if err := ecReconstructMissingDataStreamTo(w, cfg, origSize, bodies, nil); err != nil {
		return fmt.Errorf("ec join: %w", err)
	}
	return nil
}

func ecReconstructStreamDataReaders(cfg ECConfig, bodies []io.Reader) ([]io.Reader, error) {
	dataReaders := make([]io.Reader, cfg.DataShards)
	missingData := false
	for i := 0; i < cfg.DataShards; i++ {
		if bodies[i] == nil {
			missingData = true
			continue
		}
		dataReaders[i] = bodies[i]
	}
	if !missingData {
		return dataReaders, nil
	}
	return nil, nil
}

// errECReadAborted reports that a shard-body fill stopped because the consumer
// aborted (stop closed). Local teardown, never a peer fault — it must not
// reach health marking (it doesn't: producers swallow it and exit).
var errECReadAborted = errors.New("ec: shard read aborted by consumer")

// readFullOrStop fills buf like io.ReadFull but checks stop between individual
// Reads. It cannot interrupt a Read already in flight — S8-2 forbids that (see
// newECReconstructStreamReaderWithPrefetch) — so after stop is closed, a
// producer stuck on a trickling peer (progress arriving, just slowly) still
// exits at the very next stop-check instead of waiting indefinitely for the
// idle-read deadline to re-arm forever; against a fully stalled peer with no
// progress at all, the bound remains the transport idle-read deadline (≤5min,
// see http_transport.go), and it can no longer be re-armed past that because
// the producer exits on its next check regardless of trickled progress. Local
// disk reads return at syscall speed either way. stop == nil degrades to plain
// io.ReadFull semantics.
func readFullOrStop(r io.Reader, buf []byte, stop <-chan struct{}) (int, error) {
	n := 0
	for n < len(buf) {
		if stop != nil {
			select {
			case <-stop:
				return n, errECReadAborted
			default:
			}
		}
		m, err := r.Read(buf[n:])
		n += m
		if n == len(buf) {
			return n, nil
		}
		if err != nil {
			if errors.Is(err, io.EOF) && n > 0 {
				return n, io.ErrUnexpectedEOF
			}
			return n, err
		}
	}
	return n, nil
}

func ecReconstructMissingDataStreamTo(w io.Writer, cfg ECConfig, origSize int64, bodies []io.Reader, stop <-chan struct{}) error {
	enc, err := getEncoder(cfg)
	if err != nil {
		return fmt.Errorf("ec decoder: %w", err)
	}
	shardBodySize := (origSize + int64(cfg.DataShards) - 1) / int64(cfg.DataShards)
	windowSize := int64(defaultECStreamBlockSize)
	if shardBodySize < windowSize {
		windowSize = shardBodySize
	}
	if windowSize <= 0 {
		return nil
	}
	windows := make([][]byte, len(bodies))
	windowBufs := make([][]byte, len(bodies))
	for i, r := range bodies {
		if r != nil {
			windowBufs[i] = getECDataShardBuffer(int(windowSize))
		}
	}
	defer func() {
		for _, b := range windowBufs {
			if b != nil {
				putECDataShardBuffer(b)
			}
		}
	}()
	// Stream Split stores shards linearly (shard 0 = bytes [0..shardBodySize),
	// shard 1 = bytes [shardBodySize..2*shardBodySize), …). To produce the
	// original byte stream we therefore have to emit each reconstructed data
	// shard's body in full before moving to the next one. Stream shard 0
	// directly as each window is reconstructed; buffer shards 1..k-1's
	// reconstructed windows in memory for emission once shard 0 is complete.
	// Memory cost: (k-1) × shardBodySize + windowBufs. Buffers are pooled
	// via ecDataShardBufferPool so steady-state runs do not re-allocate.
	var tailOutputs [][]byte
	if cfg.DataShards > 1 {
		tailOutputs = make([][]byte, cfg.DataShards-1)
		for i := range tailOutputs {
			tailOutputs[i] = getECDataShardBuffer(int(shardBodySize))[:0]
		}
		defer func() {
			for _, b := range tailOutputs {
				putECDataShardBuffer(b)
			}
		}()
	}
	remainingShard := shardBodySize
	remainingOutput := origSize
	for remainingShard > 0 {
		n := windowSize
		if remainingShard < n {
			n = remainingShard
		}
		for i := range windows {
			windows[i] = nil
		}
		for i, r := range bodies {
			if r == nil {
				continue
			}
			buf := windowBufs[i][:n]
			if _, err := readFullOrStop(r, buf, stop); err != nil {
				return fmt.Errorf("read ec shard %d window: %w", i, err)
			}
			windows[i] = buf
		}
		if err := enc.ReconstructData(windows); err != nil {
			return fmt.Errorf("ec reconstruct: %w", err)
		}
		if windows[0] == nil {
			return fmt.Errorf("ec reconstruct: data shard 0 unavailable")
		}
		toWrite := int64(len(windows[0]))
		if remainingOutput < toWrite {
			toWrite = remainingOutput
		}
		if _, err := w.Write(windows[0][:toWrite]); err != nil {
			return err
		}
		remainingOutput -= toWrite
		for i := 1; i < cfg.DataShards; i++ {
			if windows[i] == nil {
				return fmt.Errorf("ec reconstruct: data shard %d unavailable", i)
			}
			tailOutputs[i-1] = append(tailOutputs[i-1], windows[i]...)
		}
		remainingShard -= n
	}
	for i := 1; i < cfg.DataShards && remainingOutput > 0; i++ {
		toWrite := int64(len(tailOutputs[i-1]))
		if remainingOutput < toWrite {
			toWrite = remainingOutput
		}
		if _, err := w.Write(tailOutputs[i-1][:toWrite]); err != nil {
			return err
		}
		remainingOutput -= toWrite
	}
	return nil
}

func ecReconstructBodies(cfg ECConfig, shards [][]byte) (int64, [][]byte, error) {
	if len(shards) != cfg.NumShards() {
		return 0, nil, fmt.Errorf("shard count mismatch: got %d, want %d", len(shards), cfg.NumShards())
	}
	var origSize int64 = -1
	bodies := make([][]byte, len(shards))
	for i, s := range shards {
		if s == nil {
			continue
		}
		size, body, err := decodeShardHeader(s)
		if err != nil {
			continue
		}
		if origSize < 0 {
			origSize = size
		}
		bodies[i] = body
	}
	if origSize < 0 {
		return 0, nil, fmt.Errorf("no readable shards")
	}
	return origSize, bodies, nil
}

// ecShardTruncatedError reports a shard stream that ended cleanly (EOF)
// before the expected byte count — evidence of a truncated on-disk shard or a
// peer serving a short body, distinct from transport errors. It deliberately
// does NOT wrap io.EOF/io.ErrUnexpectedEOF: asyncPrefetchReader normalizes
// the EOF family as legitimate end-of-stream, and this error must propagate.
type ecShardTruncatedError struct {
	Idx  int
	Want int64
	Got  int64
}

func (e *ecShardTruncatedError) Error() string {
	return fmt.Sprintf("ec: shard %d truncated: got %d of %d body bytes", e.Idx, e.Got, e.Want)
}

// ecShardHeaderMismatchError reports shards whose header origSize disagrees
// with the metadata-authoritative object size. Unlike the old first-shard-seed
// consensus, each listed shard is individually attributable: its header lied
// relative to the size quorum-meta recorded for this object/segment.
type ecShardHeaderMismatchError struct {
	Idxs []int
	Got  []int64
	Want int64
	// AnchorCorroborated is true iff at least one readable shard's header
	// MATCHED the anchor (Want). Only then are the Idxs shards individually
	// blameable: unanimous disagreement means the anchor is just as suspect
	// as the shards (metadata corruption or full collusion —
	// indistinguishable), so callers must NOT health-mark anyone from an
	// uncorroborated mismatch.
	AnchorCorroborated bool
}

func (e *ecShardHeaderMismatchError) Error() string {
	return fmt.Sprintf("ec: shard header size mismatch: shards %v report %v, metadata says %d", e.Idxs, e.Got, e.Want)
}

// ErrECShardIntegrity classifies the typed EC shard integrity failures
// (*ecShardTruncatedError, *ecShardHeaderMismatchError) via errors.Is: both
// implement Is(ErrECShardIntegrity). Exported so layers outside the cluster
// package (the S3 error mapper) can detect and genericize them without
// depending on the unexported types.
var ErrECShardIntegrity = errors.New("ec shard integrity error")

func (e *ecShardTruncatedError) Is(target error) bool      { return target == ErrECShardIntegrity }
func (e *ecShardHeaderMismatchError) Is(target error) bool { return target == ErrECShardIntegrity }

// IsECIntegrityError reports whether err is (or wraps) a typed EC shard
// integrity failure — a truncated shard body or a shard-header/metadata size
// mismatch. Their messages carry shard indices and per-peer byte counts, so
// the S3 error mapper replaces them with a generic message before they reach
// a client response.
func IsECIntegrityError(err error) bool { return errors.Is(err, ErrECShardIntegrity) }

// ecExactLenReader bounds a shard body to its expected length: excess bytes
// are capped (never reach the EC layer), and a clean EOF before the expected
// count becomes *ecShardTruncatedError. onFault, if non-nil, fires once on
// truncation so the caller can attribute the fault to the serving peer —
// transport (non-EOF) errors pass through untouched; the endpoint-layer
// healthTrackingReadCloser already marks those. onClean, if non-nil, fires
// once when the body is delivered to its full expected length with no fault —
// clean completion evidence for the peer that served it (see markHealthAfter
// / OpenObject's onShardClean wiring).
type ecExactLenReader struct {
	r         io.Reader
	idx       int
	want      int64
	remaining int64
	onFault   func(int)
	onClean   func(int)
}

func (g *ecExactLenReader) Read(p []byte) (int, error) {
	if g.remaining <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > g.remaining {
		p = p[:g.remaining]
	}
	n, err := g.r.Read(p)
	g.remaining -= int64(n)
	if g.remaining == 0 && g.onClean != nil &&
		(err == nil || errors.Is(err, io.EOF)) {
		// Exactly `want` bytes delivered with no transport fault — clean
		// completion (a plain EOF may arrive alongside the final bytes or on
		// the next Read; both are clean). Any error the endpoint layer counts
		// as a peer fault — including io.ErrUnexpectedEOF, which
		// isPeerFaultReadErr does NOT exempt — must NOT fire onClean: the
		// healthTrackingReadCloser just marked the peer unhealthy for it, and
		// onClean would clear that cooldown immediately.
		g.onClean(g.idx)
		g.onClean = nil
	}
	if err != nil && g.remaining > 0 &&
		(errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)) {
		if g.onFault != nil {
			g.onFault(g.idx)
			g.onFault = nil
		}
		return n, &ecShardTruncatedError{Idx: g.idx, Want: g.want, Got: g.want - g.remaining}
	}
	if err != nil && g.remaining == 0 &&
		(errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)) {
		// The body is complete; an EOF-family error delivered alongside the
		// final bytes is a normal end of stream, not a truncation.
		return n, io.EOF
	}
	return n, err
}

// ecWrapBodiesExactLen wraps every non-nil post-header shard body in an
// exact-length guard of ceil(origSize/K) bytes — the invariant the write path
// guarantees for every shard, parity and zero-padded tail included. It
// mutates bodies in place and returns the same slice.
func ecWrapBodiesExactLen(cfg ECConfig, origSize int64, bodies []io.Reader, onShardFault, onShardClean func(int)) []io.Reader {
	if cfg.DataShards <= 0 {
		return bodies
	}
	shardBodySize := (origSize + int64(cfg.DataShards) - 1) / int64(cfg.DataShards)
	for i, b := range bodies {
		if b != nil {
			bodies[i] = &ecExactLenReader{r: b, idx: i, want: shardBodySize, remaining: shardBodySize, onFault: onShardFault, onClean: onShardClean}
		}
	}
	return bodies
}

// ecReconstructStreamBodies strips and validates the 8-byte shard headers.
// expectedSize >= 0 anchors validation to the metadata-authoritative size:
// every readable header must equal it, disagreeing shards are reported (all of
// them) via *ecShardHeaderMismatchError, and the returned origSize IS
// expectedSize — so downstream buffer sizing derives from metadata, never from
// the wire. expectedSize < 0 falls back to first-shard-seed consensus (callers
// with no metadata anchor, e.g. ECReconstructStreamTo).
func ecReconstructStreamBodies(cfg ECConfig, shards []io.Reader, expectedSize int64) (int64, []io.Reader, error) {
	if len(shards) != cfg.NumShards() {
		return 0, nil, fmt.Errorf("shard count mismatch: got %d, want %d", len(shards), cfg.NumShards())
	}
	// The metadata anchor drives buffer sizing downstream — guard it with the
	// same overflow bound applied to wire headers, so corrupt metadata cannot
	// drive the ceil(size/K) math past int64 either.
	if expectedSize > math.MaxInt64-int64(cfg.NumShards()) {
		return 0, nil, fmt.Errorf("ec: metadata object size %d out of range", expectedSize)
	}
	var origSize int64 = -1
	var mismatch *ecShardHeaderMismatchError
	bodies := make([]io.Reader, len(shards))
	for i, r := range shards {
		if r == nil {
			continue
		}
		var header [shardHeaderSize]byte
		if n, err := io.ReadFull(r, header[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return 0, nil, &ecShardTruncatedError{Idx: i, Want: shardHeaderSize, Got: int64(n)}
			}
			return 0, nil, fmt.Errorf("read shard %d header: %w", i, err)
		}
		size := int64(binary.BigEndian.Uint64(header[:]))
		// The header is untrusted input from the serving peer: reject sizes
		// that are negative (uint64 wrap) or large enough to overflow the
		// ceil(origSize/K) math the exact-length guard and the missing-data
		// window loop derive from it.
		outOfRange := size < 0 || size > math.MaxInt64-int64(cfg.NumShards())
		if expectedSize >= 0 {
			// Anchored mode: ANY disagreement with metadata — including a
			// hostile out-of-range value — is individually attributable to
			// this shard. Collect instead of failing generically, so the
			// worst lies do not dodge per-shard health marking.
			if outOfRange || size != expectedSize {
				if mismatch == nil {
					mismatch = &ecShardHeaderMismatchError{Want: expectedSize}
				}
				mismatch.Idxs = append(mismatch.Idxs, i)
				mismatch.Got = append(mismatch.Got, size)
				continue // keep scanning: report ALL disagreeing shards
			}
			origSize = expectedSize
		} else {
			// Legacy consensus mode (no metadata anchor): keep the untyped
			// bounds rejection — nothing is attributable without an anchor.
			if outOfRange {
				return 0, nil, fmt.Errorf("shard %d header size %d out of range", i, size)
			}
			if origSize < 0 {
				origSize = size
			} else if size != origSize {
				return 0, nil, fmt.Errorf("shard %d original size mismatch: got %d, want %d", i, size, origSize)
			}
		}
		bodies[i] = r
	}
	if mismatch != nil {
		// origSize was set only if some shard's header matched the anchor —
		// that agreement is what makes the disagreeing shards blameable.
		mismatch.AnchorCorroborated = origSize >= 0
		return 0, nil, mismatch
	}
	if origSize < 0 {
		return 0, nil, fmt.Errorf("no readable shards")
	}
	return origSize, bodies, nil
}

// writeBuffer is an io.Writer that appends to an internal slice — used to
// avoid allocating bytes.Buffer (smaller footprint for in-memory join).
type writeBuffer struct {
	b []byte
}

func (w *writeBuffer) Write(p []byte) (int, error) {
	w.b = append(w.b, p...)
	return len(p), nil
}

// shardFilePath returns the on-disk filename used when a node stores its
// local shard of an EC-encoded object. The path is distinct from the object
// path so we can keep old N×-replicated full-object files coexisting during
// migration (Slice 5).
//
//nolint:unused // package tests pin shard path compatibility.
func shardFilePath(dataRoot, bucket, key string, shardIdx int) string {
	return filepath.Join(dataRoot, "ec-shards", bucket, key, fmt.Sprintf("shard_%d", shardIdx))
}
