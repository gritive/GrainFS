package packblob

import (
	"sync"

	"github.com/klauspost/compress/zstd"
)

var (
	encoderPool = sync.Pool{
		New: func() any {
			enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
			return enc
		},
	}
	decoderPool = sync.Pool{
		New: func() any {
			dec, _ := zstd.NewReader(nil)
			return dec
		},
	}
)

func compress(data []byte) ([]byte, error) {
	enc := encoderPool.Get().(*zstd.Encoder)
	out := enc.EncodeAll(data, nil)
	encoderPool.Put(enc)
	return out, nil
}

func decompress(data []byte) ([]byte, error) {
	dec := decoderPool.Get().(*zstd.Decoder)
	out, err := dec.DecodeAll(data, nil)
	decoderPool.Put(dec)
	if err != nil {
		return nil, err
	}
	return out, nil
}
