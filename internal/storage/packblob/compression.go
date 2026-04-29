package packblob

import (
	"github.com/gritive/GrainFS/internal/pool"
	"github.com/klauspost/compress/zstd"
)

var (
	encoderPool = pool.New(func() *zstd.Encoder {
		enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
		return enc
	})
	decoderPool = pool.New(func() *zstd.Decoder {
		dec, _ := zstd.NewReader(nil)
		return dec
	})
)

func compress(data []byte) ([]byte, error) {
	enc := encoderPool.Get()
	out := enc.EncodeAll(data, nil)
	encoderPool.Put(enc)
	return out, nil
}

func decompress(data []byte) ([]byte, error) {
	dec := decoderPool.Get()
	out, err := dec.DecodeAll(data, nil)
	decoderPool.Put(dec)
	if err != nil {
		return nil, err
	}
	return out, nil
}
