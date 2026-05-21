package cluster

import (
	"fmt"
	"testing"
)

func benchNodes(n int) []string {
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = fmt.Sprintf("node-%d", i)
	}
	return out
}

func BenchmarkPlaceShards_Nil(b *testing.B) {
	for _, N := range []int{3, 6, 12, 24} {
		for _, k := range []int{2, 6, 10} {
			b.Run(fmt.Sprintf("N=%d/k+m=%d", N, k), func(b *testing.B) {
				nodes := benchNodes(N)
				b.ResetTimer()
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					_ = PlaceShards("bucket/object/key", nodes, nil, k)
				}
			})
		}
	}
}

func BenchmarkPlaceShards_Ones(b *testing.B) {
	for _, N := range []int{3, 6, 12, 24} {
		b.Run(fmt.Sprintf("N=%d", N), func(b *testing.B) {
			nodes := benchNodes(N)
			weights := make([]float64, N)
			for i := range weights {
				weights[i] = 1
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = PlaceShards("bucket/object/key", nodes, weights, 6)
			}
		})
	}
}
