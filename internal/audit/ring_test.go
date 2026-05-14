// internal/audit/ring_test.go
package audit_test

import (
	"io"
	"testing"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

func TestRingDropOnOverflow(t *testing.T) {
	r := audit.NewRing()
	dropped := 0
	for i := 0; i < 70000; i++ {
		before := r.Drops()
		r.Put(audit.S3Event{Method: "PUT"})
		if r.Drops() > before {
			dropped++
		}
	}
	require.Greater(t, dropped, 0)
	require.Equal(t, uint64(dropped), r.Drops())
}

func TestRingDrainInto(t *testing.T) {
	r := audit.NewRing()
	for i := 0; i < 3; i++ {
		r.Put(audit.S3Event{Status: int32(i)})
	}
	buf := make([]audit.S3Event, 10)
	got := r.DrainInto(buf)
	require.Len(t, got, 3)
	for i, e := range got {
		require.Equal(t, int32(i), e.Status)
	}
	empty := r.DrainInto(buf)
	require.Empty(t, empty)
}

func BenchmarkAuditEmit(b *testing.B) {
	prev := log.Logger
	log.Logger = zerolog.New(io.Discard)
	defer func() { log.Logger = prev }()

	e := audit.NewEmitter("bench-node")
	ev := audit.S3Event{Method: "PUT", Bucket: "data", Key: "obj", Status: 200}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			e.EmitS3(ev)
		}
	})
}
