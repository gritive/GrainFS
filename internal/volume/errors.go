package volume

import "errors"

var (
	ErrPoolQuotaExceeded  = errors.New("volume pool quota exceeded")
	ErrShrinkNotSupported = errors.New("volume shrink not supported")
	ErrInvalidSize        = errors.New("invalid volume size")
)
