package putpipeline

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"hash"
	"io"

	"github.com/gritive/GrainFS/internal/storage"
)

// IngestActor reads an HTTP body into stripe-sized chunks and pushes
// them onto its out channel. One IngestActor goroutine per PUT.
type IngestActor struct {
	out         chan<- StripePlaintext
	stripeBytes int
}

var errInvalidStripeBytes = errors.New("ingest actor: stripeBytes must be > 0")

// Run executes the ingest loop for one PUT. Returns the final ETag
// (MD5 hex for external buckets, empty for internal/unnamed), total
// bytes read, and any error. Out channel close is the caller's
// responsibility (Pipeline.Put coordinates it).
func (a *IngestActor) Run(ctx context.Context, putID uint64, bucket string, body io.Reader) (etag string, total int64, err error) {
	if a.stripeBytes <= 0 {
		return "", 0, errInvalidStripeBytes
	}
	h := newHashForBucket(bucket)
	var src io.Reader = body
	if h != nil {
		src = io.TeeReader(body, h)
	}
	// Wrap in bufio so we can peek one byte after a full read to detect
	// whether the stream is exhausted (handles exact-multiple body sizes
	// where io.ReadFull returns (stripeBytes, nil) on the last stripe).
	br := bufio.NewReaderSize(src, 4096)

	stripeIdx := uint32(0)
	for {
		buf := make([]byte, a.stripeBytes)
		n, readErr := io.ReadFull(br, buf)
		if n == 0 && (readErr == io.EOF || readErr == nil) {
			break
		}
		last := readErr == io.ErrUnexpectedEOF || readErr == io.EOF
		// For an exact-multiple body, io.ReadFull returns (stripeBytes, nil).
		// Peek one byte to determine whether the stream is now exhausted.
		if !last && readErr == nil {
			_, peekErr := br.Peek(1)
			if peekErr == io.EOF {
				last = true
			}
		}
		var padding uint32
		if n < a.stripeBytes {
			padding = uint32(a.stripeBytes - n)
			for i := n; i < a.stripeBytes; i++ {
				buf[i] = 0
			}
		}
		s := StripePlaintext{
			PutID:     putID,
			StripeIdx: stripeIdx,
			Data:      buf,
			Padding:   padding,
			LastInPut: last,
		}
		select {
		case <-ctx.Done():
			return "", total, ctx.Err()
		case a.out <- s:
		}
		total += int64(n)
		stripeIdx++
		if last {
			break
		}
		if readErr != nil && readErr != io.ErrUnexpectedEOF {
			return "", total, readErr
		}
	}
	if h != nil {
		etag = hex.EncodeToString(h.Sum(nil))
	}
	return etag, total, nil
}

// newHashForBucket picks the streaming hash used to build the object's
// ETag. External (S3-visible) buckets must use MD5 because the AWS S3
// protocol expects the ETag of a single PUT to equal Content-MD5.
// Internal buckets and the empty bucket return nil (no hash); their
// ETag is filled by the storage layer elsewhere.
func newHashForBucket(bucket string) hash.Hash {
	if bucket == "" {
		return nil
	}
	if storage.IsInternalBucket(bucket) {
		return nil
	}
	return md5.New()
}
