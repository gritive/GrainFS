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

func TestService_Apply_ValidXML_CallsProposer(t *testing.T) {
	prop := &fakeProposer{}
	svc := NewService(NewStore(newTestDB(t)), prop, &fakeLeadership{}, nil, nil, 0)
	raw := []byte(`<LifecycleConfiguration><Rule><ID>r1</ID><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>`)
	require.NoError(t, svc.Apply(context.Background(), "b", raw))
	require.Len(t, prop.putCalls, 1)
	assert.Equal(t, "b", prop.putCalls[0].Bucket)
	assert.Equal(t, raw, prop.putCalls[0].Raw)
}

func TestService_Apply_InvalidXML_ReturnsError(t *testing.T) {
	prop := &fakeProposer{}
	svc := NewService(NewStore(newTestDB(t)), prop, &fakeLeadership{}, nil, nil, 0)
	err := svc.Apply(context.Background(), "b", []byte("not xml at all"))
	require.Error(t, err)
	assert.Empty(t, prop.putCalls, "proposer must not be called on invalid XML")
}

func TestService_Apply_FailsValidation(t *testing.T) {
	prop := &fakeProposer{}
	svc := NewService(NewStore(newTestDB(t)), prop, &fakeLeadership{}, nil, nil, 0)
	raw := []byte(`<LifecycleConfiguration></LifecycleConfiguration>`)
	err := svc.Apply(context.Background(), "b", raw)
	require.Error(t, err)
	assert.Empty(t, prop.putCalls)
}
