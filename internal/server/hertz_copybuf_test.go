package server

import (
	"testing"

	hertzutils "github.com/cloudwego/hertz/pkg/common/utils"
	"github.com/stretchr/testify/require"
)

func TestHertzResponseCopyBufferSizedForMultipartRanges(t *testing.T) {
	buf := hertzutils.CopyBufPool.Get().([]byte)
	defer hertzutils.CopyBufPool.Put(buf)

	require.GreaterOrEqual(t, len(buf), hertzResponseCopyBufferSize)
	require.GreaterOrEqual(t, len(buf), 64*1024)
}
