package datawal_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/datawal"
)

func BenchmarkEncodeRecord(b *testing.B) {
	sizes := []int{1024, 64 * 1024, 1024 * 1024}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			payload := make([]byte, size)
			_, _ = io.ReadFull(rand.Reader, payload)
			rec := datawal.Record{
				Seq:       123,
				Timestamp: 456,
				Op:        datawal.OpSegmentPut,
				Bucket:    "bucket-name",
				Key:       "key-name/with/some/slashes/and/extensions.bin",
				Target:    "target-segment-name-xyz",
				Offset:    1024 * 1024,
				Size:      int64(size),
				Payload:   payload,
			}
			b.ResetTimer()
			b.ReportAllocs()
			var buf bytes.Buffer
			for i := 0; i < b.N; i++ {
				buf.Reset()
				_ = datawal.EncodeRecord(&buf, rec)
			}
		})
	}
}

func BenchmarkDecodeRecord(b *testing.B) {
	sizes := []int{1024, 64 * 1024, 1024 * 1024}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			payload := make([]byte, size)
			_, _ = io.ReadFull(rand.Reader, payload)
			rec := datawal.Record{
				Seq:       123,
				Timestamp: 456,
				Op:        datawal.OpSegmentPut,
				Bucket:    "bucket-name",
				Key:       "key-name/with/some/slashes/and/extensions.bin",
				Target:    "target-segment-name-xyz",
				Offset:    1024 * 1024,
				Size:      int64(size),
				Payload:   payload,
			}
			var buf bytes.Buffer
			_ = datawal.EncodeRecord(&buf, rec)
			data := buf.Bytes()
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = datawal.DecodeRecord(bytes.NewReader(data))
			}
		})
	}
}

func BenchmarkWALAppend(b *testing.B) {
	sizes := []int{1024, 64 * 1024}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			payload := make([]byte, size)
			_, _ = io.ReadFull(rand.Reader, payload)
			dir := b.TempDir()
			w, err := datawal.Open(dir, nil, "datawal")
			if err != nil {
				b.Fatal(err)
			}
			defer w.Close()

			rec := datawal.Record{
				Op:      datawal.OpSegmentPut,
				Bucket:  "bucket-name",
				Key:     "key-name",
				Payload: payload,
			}
			ctx := context.Background()
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = w.Append(ctx, rec)
			}
		})
	}
}

func BenchmarkWALAppendEncrypted(b *testing.B) {
	sizes := []int{1024, 64 * 1024}
	clusterID := bytes.Repeat([]byte{0xC1}, 16)
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x51}, encrypt.KEKSize), clusterID)
	if err != nil {
		b.Fatal(err)
	}
	sealer := storage.NewDEKKeeperAdapter(keeper, clusterID)
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			payload := make([]byte, size)
			_, _ = io.ReadFull(rand.Reader, payload)
			dir := filepath.Join(b.TempDir(), "datawal")
			w, err := datawal.Open(dir, sealer, "datawal")
			if err != nil {
				b.Fatal(err)
			}
			defer w.Close()

			rec := datawal.Record{
				Op:      datawal.OpSegmentPut,
				Bucket:  "bucket-name",
				Key:     "key-name",
				Payload: payload,
			}
			ctx := context.Background()
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = w.Append(ctx, rec)
			}
		})
	}
}
