package lifecycle

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeProposer captures the last propose call for assertions.
type fakeProposer struct {
	putCalls []struct {
		Bucket string
		Raw    []byte
	}
	deleteCalls []string
	err         error
}

func (f *fakeProposer) ProposeLifecyclePut(ctx context.Context, bucket string, raw []byte) error {
	f.putCalls = append(f.putCalls, struct {
		Bucket string
		Raw    []byte
	}{bucket, append([]byte(nil), raw...)})
	return f.err
}
func (f *fakeProposer) ProposeLifecycleDelete(ctx context.Context, bucket string) error {
	f.deleteCalls = append(f.deleteCalls, bucket)
	return f.err
}

type fakeLeadership struct {
	leader bool
}

func (f *fakeLeadership) IsLeader() bool { return f.leader }
func (f *fakeLeadership) Subscribe() (<-chan struct{}, func()) {
	ch := make(chan struct{})
	return ch, func() {}
}

func newServiceForTest(t *testing.T) *Service {
	t.Helper()
	return NewService(NewStore(newTestDB(t)), &fakeProposer{}, &fakeLeadership{}, nil, nil, 0)
}

func TestService_Enabled_True(t *testing.T) {
	svc := newServiceForTest(t)
	assert.True(t, svc.Enabled())
}

func TestService_Get_NotFound_ReturnsNil(t *testing.T) {
	svc := newServiceForTest(t)
	cfg, err := svc.Get("nope")
	require.NoError(t, err)
	assert.Nil(t, cfg)
}

func TestService_GetRaw_NotFound_ReturnsNil(t *testing.T) {
	svc := newServiceForTest(t)
	raw, err := svc.GetRaw("nope")
	require.NoError(t, err)
	assert.Nil(t, raw)
}
