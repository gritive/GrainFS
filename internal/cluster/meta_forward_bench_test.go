package cluster

import (
	"fmt"
	"testing"
)

// BenchmarkMetaForwardRequest_RoundTrip measures the cost of encode +
// decode for a typical metaForwardRequest. The Command bytes dominate
// payload size; sizes chosen to bracket realistic raft commands.
func BenchmarkMetaForwardRequest_RoundTrip(b *testing.B) {
	sizes := []int{256, 4 << 10, 64 << 10}
	for _, size := range sizes {
		size := size
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			cmd := make([]byte, size)
			for i := range cmd {
				cmd[i] = byte(i)
			}
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				payload := encodeMetaForwardRequest(cmd, nil)
				_, _, _, err := decodeMetaForwardRequest(payload)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkMetaForwardReply_RoundTrip measures encode+decode for the
// reply path. Replies are tiny (Index + optional error) — the overhead
// here is pure JSON wrapper cost, not payload cost.
func BenchmarkMetaForwardReply_RoundTrip(b *testing.B) {
	b.Run("success", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			payload := encodeMetaForwardReplyWithIndex(uint64(i), nil)
			_, err := decodeMetaForwardReplyWithIndex(payload)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("error", func(b *testing.B) {
		boom := fmt.Errorf("simulated cluster error")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			payload := encodeMetaForwardReplyWithIndex(0, boom)
			_, err := decodeMetaForwardReplyWithIndex(payload)
			if err == nil {
				b.Fatal("expected error")
			}
		}
	})
}
