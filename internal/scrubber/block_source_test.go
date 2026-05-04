package scrubber

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type mockBlockSource struct {
	name   string
	blocks []Block
}

func (m *mockBlockSource) Name() string { return m.name }
func (m *mockBlockSource) Iter(ctx context.Context, scope ScrubScope, keyPrefix string) (<-chan Block, error) {
	ch := make(chan Block, len(m.blocks))
	go func() {
		defer close(ch)
		for _, b := range m.blocks {
			select {
			case <-ctx.Done():
				return
			case ch <- b:
			}
		}
	}()
	return ch, nil
}

type mockBlockVerifier struct {
	verifyResults map[string]BlockStatus
}

func (m *mockBlockVerifier) Verify(ctx context.Context, b Block) (BlockStatus, error) {
	return m.verifyResults[b.Key], nil
}
func (m *mockBlockVerifier) Repair(ctx context.Context, b Block) error { return nil }

func TestBlockSource_InterfaceContract(t *testing.T) {
	src := &mockBlockSource{
		name:   "mock",
		blocks: []Block{{Bucket: "b", Key: "k1"}, {Bucket: "b", Key: "k2"}},
	}
	require.Equal(t, "mock", src.Name())
	ch, err := src.Iter(context.Background(), ScopeFull, "")
	require.NoError(t, err)
	var got []Block
	for b := range ch {
		got = append(got, b)
	}
	require.Len(t, got, 2)
}

func TestBlockStatus_IsHealthy(t *testing.T) {
	require.True(t, BlockStatus{Healthy: true}.IsHealthy())
	require.False(t, BlockStatus{Corrupt: true}.IsHealthy())
}
