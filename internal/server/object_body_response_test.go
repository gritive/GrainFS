package server

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

type rawOnlyReadCloser struct {
	body []byte
}

func (r rawOnlyReadCloser) Read([]byte) (int, error) {
	return 0, errors.New("raw body path should not read")
}

func (r rawOnlyReadCloser) Close() error {
	return nil
}

func (r rawOnlyReadCloser) RawBody() []byte {
	return r.body
}

func TestWriteObjectBody_StreamsSmallObject(t *testing.T) {
	data := bytes.Repeat([]byte("a"), 1024)
	c := app.NewContext(0)
	obj := &storage.Object{
		Key:         "small.bin",
		Size:        int64(len(data)),
		ContentType: "text/plain",
	}

	streamed, err := writeObjectBody(c, io.NopCloser(bytes.NewReader(data)), obj, "")
	require.NoError(t, err)
	require.True(t, streamed, "all non-empty objects should stream (no size threshold)")
	require.True(t, c.Response.IsBodyStream())
}

func TestWriteObjectBody_StreamsWarpSizedObject(t *testing.T) {
	data := bytes.Repeat([]byte("G"), 64*1024)
	c := app.NewContext(0)
	obj := &storage.Object{
		Key:         "warp-sized.bin",
		Size:        int64(len(data)),
		ContentType: "application/octet-stream",
	}

	streamed, err := writeObjectBody(c, io.NopCloser(bytes.NewReader(data)), obj, "")
	require.NoError(t, err)
	require.True(t, streamed, "every non-empty object streams (no size threshold)")
	require.True(t, c.Response.IsBodyStream())
	require.Equal(t, consts.StatusOK, c.Response.StatusCode())
}

// Zero-byte objects still take the buffered branch, where the RawBody zero-copy
// shortcut avoids reading through the reader (its Read would error).
func TestWriteObjectBody_EmptyObjectUsesRawBodyWithoutRead(t *testing.T) {
	c := app.NewContext(0)
	obj := &storage.Object{
		Key:         "empty.bin",
		Size:        0,
		ContentType: "application/octet-stream",
	}

	streamed, err := writeObjectBody(c, rawOnlyReadCloser{body: nil}, obj, "")
	require.NoError(t, err)
	require.False(t, streamed)
	require.False(t, c.Response.IsBodyStream())
	require.Equal(t, consts.StatusOK, c.Response.StatusCode())
	require.Empty(t, c.Response.Body())
}

func BenchmarkWriteObjectBody_WarpSizedObject(b *testing.B) {
	data := bytes.Repeat([]byte("G"), 64*1024)
	obj := &storage.Object{
		Key:         "warp-sized.bin",
		Size:        int64(len(data)),
		ContentType: "application/octet-stream",
	}

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c := app.NewContext(0)
		streamed, err := writeObjectBody(c, io.NopCloser(bytes.NewReader(data)), obj, "")
		if err != nil {
			b.Fatal(err)
		}
		if !streamed {
			b.Fatal("expected streamed response")
		}
	}
}

// TestExactLengthReadCloser_ShortBackendSurfacesUnexpectedEOF pins the
// exact-length contract: a backend stream that ends cleanly short of the
// advertised length must surface io.ErrUnexpectedEOF, never a silent short
// body under a larger Content-Length.
func TestExactLengthReadCloser_ShortBackendSurfacesUnexpectedEOF(t *testing.T) {
	rc := newExactLengthReadCloser(io.NopCloser(strings.NewReader("short")), 10)
	got, err := io.ReadAll(rc)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("want io.ErrUnexpectedEOF for short backend, got %v", err)
	}
	if string(got) != "short" {
		t.Fatalf("delivered prefix = %q", got)
	}

	// Exact-length stream stays clean.
	rc = newExactLengthReadCloser(io.NopCloser(strings.NewReader("exactlyten")), 10)
	got, err = io.ReadAll(rc)
	if err != nil || string(got) != "exactlyten" {
		t.Fatalf("exact-length read: got %q, err %v", got, err)
	}
}
