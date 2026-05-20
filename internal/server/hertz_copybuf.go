package server

import (
	"sync"

	hertzutils "github.com/cloudwego/hertz/pkg/common/utils"
)

const hertzResponseCopyBufferSize = 64 * 1024

func init() {
	hertzutils.CopyBufPool = sync.Pool{
		New: func() any {
			return make([]byte, hertzResponseCopyBufferSize)
		},
	}
}
