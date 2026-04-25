package cluster

import (
	"fmt"
	"testing"
	"time"
)

type slowInvalidator struct {
	delay time.Duration
}

func (s *slowInvalidator) Invalidate(_, _ string) {
	time.Sleep(s.delay)
}

func BenchmarkInvalidateAll(b *testing.B) {
	for _, n := range []int{1, 5, 10, 20} {
		b.Run(fmt.Sprintf("vfs=%d", n), func(b *testing.B) {
			r := NewRegistry()
			for i := range n {
				r.Register(fmt.Sprintf("vfs%d", i), &slowInvalidator{delay: 5 * time.Millisecond})
			}
			b.ResetTimer()
			for b.Loop() {
				r.InvalidateAll("bucket", "key")
			}
		})
	}
}
