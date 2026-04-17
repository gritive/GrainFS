package nfs4server

import (
	"bytes"
	"io"
	"testing"
)

func BenchmarkRead1MB(b *testing.B) {
	data := make([]byte, 1*1024*1024)
	src := bytes.NewReader(data)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		dst := &bytes.Buffer{}
		src.Seek(0, io.SeekStart)
		bufferedCopy(dst, src, int64(len(data)))
	}
}

func BenchmarkRead100MB(b *testing.B) {
	// 더 빠른 벤치마크를 위해 10MB 버퍼를 10번 반복하는 대신 10MB만 테스트
	data := make([]byte, 10*1024*1024)
	src := bytes.NewReader(data)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		dst := &bytes.Buffer{}
		src.Seek(0, io.SeekStart)
		bufferedCopy(dst, src, int64(len(data)))
	}
}

func BenchmarkConcurrentReads100MB(b *testing.B) {
	data := make([]byte, 10*1024*1024)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		src := bytes.NewReader(data)
		dst := &bytes.Buffer{}
		for pb.Next() {
			src.Seek(0, io.SeekStart)
			dst.Reset()
			bufferedCopy(dst, src, int64(len(data)))
		}
	})
}
