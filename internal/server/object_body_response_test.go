package server

import (
	"bytes"
	"errors"
	"io"
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

func TestWriteObjectBody_BuffersWarpSizedObject(t *testing.T) {
	data := bytes.Repeat([]byte("G"), 64*1024)
	c := app.NewContext(0)
	obj := &storage.Object{
		Key:         "warp-sized.bin",
		Size:        int64(len(data)),
		ContentType: "application/octet-stream",
	}

	streamed, err := writeObjectBody(c, io.NopCloser(bytes.NewReader(data)), obj, "")
	require.NoError(t, err)
	require.False(t, streamed, "64KiB warp-sized bodies should use buffered response path")
	require.False(t, c.Response.IsBodyStream())
	require.Equal(t, consts.StatusOK, c.Response.StatusCode())
	require.Equal(t, data, c.Response.Body())
}

func TestWriteObjectBody_UsesRawBodyWithoutRead(t *testing.T) {
	data := bytes.Repeat([]byte("G"), 64*1024)
	c := app.NewContext(0)
	obj := &storage.Object{
		Key:         "warp-sized.bin",
		Size:        int64(len(data)),
		ContentType: "application/octet-stream",
	}

	streamed, err := writeObjectBody(c, rawOnlyReadCloser{body: data}, obj, "")
	require.NoError(t, err)
	require.False(t, streamed)
	require.False(t, c.Response.IsBodyStream())
	require.Equal(t, consts.StatusOK, c.Response.StatusCode())
	require.Equal(t, data, c.Response.Body())
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
		if streamed {
			b.Fatal("unexpected streamed response")
		}
	}
}

func BenchmarkWriteObjectBody_RawWarpSizedObject(b *testing.B) {
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
		streamed, err := writeObjectBody(c, rawOnlyReadCloser{body: data}, obj, "")
		if err != nil {
			b.Fatal(err)
		}
		if streamed {
			b.Fatal("unexpected streamed response")
		}
	}
}
