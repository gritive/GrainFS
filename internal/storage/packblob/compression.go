package packblob

import "github.com/gritive/GrainFS/internal/storage/zstdpool"

func compress(data []byte) ([]byte, error)   { return zstdpool.Compress(data) }
func decompress(data []byte) ([]byte, error) { return zstdpool.Decompress(data) }
