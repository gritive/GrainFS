package putpipeline

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
)

// IngestActor reads an HTTP body into stripe-sized chunks and pushes
// them onto its out channel. One IngestActor goroutine per PUT.
//
// MD5 is always computed for external buckets to satisfy the AWS S3
// contract that a single-PUT ETag equals MD5(body). When precomputedETag
// is non-empty (client supplied Content-MD5), the actor verifies the
// computed MD5 matches and rejects on mismatch (S3 BadDigest semantics).
// MD5 runs in a sidecar goroutine so it overlaps with the pipeline's
// EC + encrypt + write stages instead of sitting on the read loop's
// critical path.
type IngestActor struct {
	out             chan<- StripePlaintext
	stripeBytes     int
	precomputedETag string
}

var errInvalidStripeBytes = errors.New("ingest actor: stripeBytes must be > 0")

// ErrContentMD5Mismatch is returned when the body's actual MD5 doesn't
// match the client-supplied Content-MD5. Maps to S3 400 BadDigest.
var ErrContentMD5Mismatch = errors.New("ingest actor: Content-MD5 mismatch")

// Run executes the ingest loop for one PUT. Returns the final ETag
// (MD5 hex for external buckets, empty for internal/unnamed), total
// bytes read, and any error. Out channel close is the caller's
// responsibility (Pipeline.Put coordinates it).
func (a *IngestActor) Run(ctx context.Context, putID uint64, bucket string, body io.Reader) (etag string, total int64, err error) {
	if a.stripeBytes <= 0 {
		return "", 0, errInvalidStripeBytes
	}
	h := newHashForBucket(bucket)

	// Sidecar MD5 goroutine. Drains md5In and computes MD5; the result
	// arrives on md5Out when md5In is closed. Keeps md5.Write off the
	// read loop's critical path so the pipeline can advance while
	// hashing happens in parallel.
	var md5In chan []byte
	md5Out := make(chan string, 1)
	if h != nil {
		md5In = make(chan []byte, 8)
		go func() {
			for chunk := range md5In {
				_, _ = h.Write(chunk)
			}
			md5Out <- hex.EncodeToString(h.Sum(nil))
		}()
	}
	cleanup := func() {
		if md5In != nil {
			close(md5In)
			<-md5Out
		}
	}

	// Wrap in bufio so we can peek one byte after a full read to detect
	// whether the stream is exhausted (handles exact-multiple body sizes
	// where io.ReadFull returns (stripeBytes, nil) on the last stripe).
	br := bufio.NewReaderSize(body, 4096)

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
		if md5In != nil {
			// Send the read bytes (excluding zero padding) to MD5
			// goroutine. The stripe buffer remains owned by downstream
			// (CPUPool); MD5 reads from the same backing array but the
			// pipeline never mutates it before reaching DriveActor.
			md5In <- buf[:n]
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
			cleanup()
			return "", total, ctx.Err()
		case a.out <- s:
		}
		total += int64(n)
		stripeIdx++
		if last {
			break
		}
		if readErr != nil && readErr != io.ErrUnexpectedEOF {
			cleanup()
			return "", total, readErr
		}
	}
	if md5In != nil {
		close(md5In)
		etag = <-md5Out
	}
	// If the client supplied a Content-MD5, verify the body's actual
	// MD5 matches. Mismatch = S3 BadDigest.
	if a.precomputedETag != "" && etag != "" && etag != a.precomputedETag {
		return "", total, fmt.Errorf("%w: client supplied %s, body computed %s", ErrContentMD5Mismatch, a.precomputedETag, etag)
	}
	return etag, total, nil
}

// newHashForBucket returns the streaming hash used to build the
// object's ETag. The pipeline only ever serves external (S3-visible)
// buckets — ShouldUseActor routes internal-bucket PUTs to the legacy
// path — so the hash is always MD5, matching the AWS S3 contract that
// a single-PUT ETag equals the object's Content-MD5. The empty bucket
// name (which a real PUT never has) returns nil defensively.
//
// Deliberately does NOT branch on IsInternalBucket: internal-bucket
// classification is a routing concern handled in ShouldUseActor, never
// a hash-skip shortcut here (see internal/storage/bucket.go's guard
// doc and TODOS.md "Hash 정책 분기 가드").
func newHashForBucket(bucket string) hash.Hash {
	if bucket == "" {
		return nil
	}
	return md5.New()
}
