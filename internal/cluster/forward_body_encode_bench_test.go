package cluster

import (
	"bytes"
	"fmt"
	"testing"
)

// BenchmarkForwardBodyEncode quantifies the cost the forward stream floor
// (minForwardStreamBytes) trades: the FRAME path reads the whole body into a
// []byte (readBoundedBody) and copies it into the args FlatBuffer
// (buildPutObjectArgsWithSSE → CreateByteVector); the STREAM path builds a
// body-less args FlatBuffer and lets the transport stream the body. The
// per-size B/op and allocs/op show how much memory a forwarded PUT/UploadPart
// of that size buffers when it takes the single-frame path on the SENDER.
//
// SCOPE: this measures only the sender-side buffering component, so it trivially
// favours streaming and does NOT by itself decide the floor. It excludes the
// Hertz HTTP request/response processing AND the receiver-side body handling —
// where the frame path also materialises the body (the body rides in the
// request FlatBuffer, so Hertz buffers the whole request before the handler
// slices BodyBytes) while the stream path streams it. A definitive 1-vs-5-MiB
// crossover under the real transport + concurrency needs an end-to-end forward
// bench (the loopback HTTPTransport harness in forward_native_wire_test.go, or
// the multi-node cluster bench).
func BenchmarkForwardBodyEncode(b *testing.B) {
	const maxBody = DefaultMaxForwardBodyBytes
	sizes := []int{256 << 10, 1 << 20, 2 << 20, 4 << 20, 5 << 20, 8 << 20}
	for _, n := range sizes {
		buf := bytes.Repeat([]byte("x"), n)
		label := fmt.Sprintf("%dKiB", n>>10)

		b.Run("frame/"+label, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				body, err := readBoundedBody(bytes.NewReader(buf), maxBody)
				if err != nil {
					b.Fatal(err)
				}
				_ = buildPutObjectArgsWithSSE("bk", "k", "application/octet-stream",
					body, "", nil, "", 0, versioningStateUnknown)
			}
		})

		b.Run("stream/"+label, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				// Body-less args; the body would be streamed by the transport.
				_ = buildPutObjectArgsWithSSE("bk", "k", "application/octet-stream",
					nil, "", nil, "", 0, versioningStateUnknown)
			}
		})
	}
}
