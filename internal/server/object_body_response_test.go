package server

import (
	"bytes"
	"io"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

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
