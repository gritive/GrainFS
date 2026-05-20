// Package storage — SegmentWriter implements the streaming PUT pipeline:
// chunker → bounded worker pool → aggregator. Memory is bounded by
// chunkSize * (workers + 1) regardless of object size.
package storage

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// DefaultChunkSize is the standard segment size for streaming PUTs.
const DefaultChunkSize = 16 << 20 // 16 MiB

// DefaultPutWorkers is the per-request concurrency for segment writes.
const DefaultPutWorkers = 8

type segmentWriteResult struct {
	idx int
	ref SegmentRef
	err error
}

// SegmentWriter streams an io.Reader through a chunker, worker pool, and
// aggregator to build an Object made of N SegmentRefs.
type SegmentWriter struct {
	backend   segmentWriterBackend
	chunkSize int
	workers   int
}

// segmentWriterBackend abstracts the storage primitive so the writer is
// testable without a full LocalBackend.
type segmentWriterBackend interface {
	WriteSegment(ctx context.Context, bucket, key string, idx int, r io.Reader) (SegmentRef, error)
}

type segmentBytesWriterBackend interface {
	WriteSegmentBytes(ctx context.Context, bucket, key string, idx int, data []byte) (SegmentRef, error)
}

// NewSegmentWriter constructs a writer with the standard defaults.
func NewSegmentWriter(b segmentWriterBackend) *SegmentWriter {
	return &SegmentWriter{backend: b, chunkSize: DefaultChunkSize, workers: DefaultPutWorkers}
}

// NewSegmentWriterWithChunkSize constructs a writer with a custom chunk size.
func NewSegmentWriterWithChunkSize(b segmentWriterBackend, chunkSize int) *SegmentWriter {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	return &SegmentWriter{backend: b, chunkSize: chunkSize, workers: DefaultPutWorkers}
}

// Write streams r, returning an Object with segments populated and
// Object.ETag set to md5(plaintext_full) — the simple-PUT rule.
//
// On any worker or chunker error, the entire PUT fails: the caller MUST NOT
// persist the returned Object (which is nil). Any segment blobs already
// written to disk become orphans cleaned up by the scrubber.
func (w *SegmentWriter) Write(ctx context.Context, bucket, key, contentType string, r io.Reader) (*Object, error) {
	wholeMD5 := md5.New()
	tee := io.TeeReader(r, wholeMD5)

	workCh := make(chan chunkJob, w.workers)
	resultCh := make(chan segmentWriteResult, w.workers)

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < w.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range workCh {
				var (
					ref SegmentRef
					err error
				)
				if bw, ok := w.backend.(segmentBytesWriterBackend); ok {
					ref, err = bw.WriteSegmentBytes(cctx, bucket, key, job.idx, job.body)
				} else {
					ref, err = w.backend.WriteSegment(cctx, bucket, key, job.idx, bytes.NewReader(job.body))
				}
				resultCh <- segmentWriteResult{idx: job.idx, ref: ref, err: err}
			}
		}()
	}

	chunkerErr := make(chan error, 1)
	go func() {
		firstChunkSize, exactFirstChunk := firstChunkBufferSize(r, w.chunkSize)
		chunkerErr <- w.chunkLoop(cctx, tee, workCh, firstChunkSize, exactFirstChunk)
		close(workCh)
	}()

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	results := make([]segmentWriteResult, 0, 64)
	var firstErr error
	for res := range resultCh {
		if res.err != nil && firstErr == nil {
			firstErr = res.err
			cancel()
		}
		results = append(results, res)
	}
	if cerr := <-chunkerErr; cerr != nil && firstErr == nil {
		// Surface context.Canceled only if no upstream error was the trigger.
		if !errors.Is(cerr, context.Canceled) {
			firstErr = cerr
		}
	}

	if firstErr != nil {
		return nil, firstErr
	}

	sort.Slice(results, func(i, j int) bool { return results[i].idx < results[j].idx })
	segs := make([]SegmentRef, len(results))
	var totalSize int64
	for i, res := range results {
		segs[i] = res.ref
		totalSize += res.ref.Size
	}

	obj := &Object{
		Key:          key,
		Size:         totalSize,
		ContentType:  contentType,
		ETag:         hex.EncodeToString(wholeMD5.Sum(nil)),
		LastModified: time.Now().Unix(),
		Segments:     segs,
	}
	return obj, nil
}

type chunkJob struct {
	idx  int
	body []byte
}

// chunkLoop reads from r in chunkSize blocks and emits chunkJob values until EOF.
//
// Empty-object case: zero-byte input must still produce ONE empty trailing
// segment so Object.Segments is never empty.
//
// Error handling: we fill the buffer with a manual io.Read loop (not
// io.ReadFull) so we can distinguish a clean io.EOF from the underlying
// reader (end-of-stream) from any other error (truncated input,
// io.ErrUnexpectedEOF surfaced by the upstream, etc.). io.ReadFull would
// rewrite EOF→ErrUnexpectedEOF mid-buffer, making real ErrUnexpectedEOF
// errors from the source indistinguishable from a normal short final chunk.
func (w *SegmentWriter) chunkLoop(ctx context.Context, r io.Reader, workCh chan<- chunkJob, firstChunkSize int, exactFirstChunk bool) error {
	idx := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if idx == 0 && exactFirstChunk && firstChunkSize == 0 {
			select {
			case workCh <- chunkJob{idx: idx, body: nil}:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		}
		chunkSize := w.chunkSize
		if idx == 0 && firstChunkSize > 0 && firstChunkSize < chunkSize {
			chunkSize = firstChunkSize
		}
		body := make([]byte, chunkSize)
		n, readErr := fillChunk(r, body)
		// Empty-object case: first iteration, no bytes, clean EOF.
		if n == 0 && errors.Is(readErr, io.EOF) && idx == 0 {
			select {
			case workCh <- chunkJob{idx: idx, body: nil}:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		}
		if n > 0 {
			body = body[:n]
			select {
			case workCh <- chunkJob{idx: idx, body: body}:
			case <-ctx.Done():
				return ctx.Err()
			}
			idx++
			if exactFirstChunk {
				return nil
			}
		}
		// Clean end-of-stream from the source.
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func firstChunkBufferSize(r io.Reader, defaultSize int) (int, bool) {
	switch rr := r.(type) {
	case *bytes.Reader:
		n := rr.Len()
		return n, n <= defaultSize
	case *bytes.Buffer:
		n := rr.Len()
		return n, n <= defaultSize
	case *strings.Reader:
		n := rr.Len()
		return n, n <= defaultSize
	}
	return defaultSize, false
}

// fillChunk reads up to len(buf) bytes from r using a plain io.Read loop.
// Returns (n, io.EOF) when the source reaches a clean end (whether before or
// after partially filling the buffer) and (n, err) for any non-EOF error.
// Unlike io.ReadFull, EOF is never rewritten to io.ErrUnexpectedEOF, so a real
// ErrUnexpectedEOF from the source is preserved verbatim.
func fillChunk(r io.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
