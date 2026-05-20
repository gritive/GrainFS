package iam

import (
	"context"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// fakeProposer captures each Propose* call as a string so tests can
// assert the dispatch sequence without spinning up Raft.
type fakeProposer struct {
	store        *Store
	enc          *encrypt.Encryptor
	calls        []string
	dispatched   []string
	saCreateErr  error // if non-nil, ProposeSACreate returns this
	keyCreateErr error // if non-nil, ProposeKeyCreate returns this

	bucketUpstreamPuts       []BucketUpstream
	bucketUpstreamDeletes    []string
	bucketUpstreamCutovers   []string
	bucketUpstreamPutErr     error
	bucketUpstreamDeleteErr  error
	bucketUpstreamCutoverErr error
}

func newFakeProposer() *fakeProposer { return &fakeProposer{} }

func (f *fakeProposer) calledSADelete(saID string) bool {
	for _, c := range f.calls {
		if c == "SADelete:"+saID {
			return true
		}
	}
	return false
}

func (f *fakeProposer) calledKeyCreate(ak string) bool {
	for _, c := range f.calls {
		if c == "KeyCreate:"+ak {
			return true
		}
	}
	return false
}

func (f *fakeProposer) calledKeyRevoke(ak string) bool {
	for _, c := range f.calls {
		if c == "KeyRevoke:"+ak {
			return true
		}
	}
	return false
}

func (f *fakeProposer) ProposeSACreate(ctx context.Context, sa ServiceAccount) error {
	f.calls = append(f.calls, "SACreate:"+sa.ID)
	f.dispatched = append(f.dispatched, "SACreate")
	if f.saCreateErr != nil {
		return f.saCreateErr
	}
	if f.store != nil {
		f.store.applySACreate(sa)
	}
	return nil
}
func (f *fakeProposer) ProposeSADelete(ctx context.Context, saID string) error {
	f.calls = append(f.calls, "SADelete:"+saID)
	f.dispatched = append(f.dispatched, "SADelete")
	return nil
}
func (f *fakeProposer) ProposeKeyCreate(ctx context.Context, k AccessKey) error {
	f.calls = append(f.calls, "KeyCreate:"+k.AccessKey)
	f.dispatched = append(f.dispatched, "KeyCreate")
	if f.keyCreateErr != nil {
		return f.keyCreateErr
	}
	if f.store != nil {
		f.store.applyKeyCreate(k)
	}
	return nil
}
func (f *fakeProposer) ProposeKeyCreateScoped(ctx context.Context, k AccessKey) error {
	f.calls = append(f.calls, "KeyCreateScoped:"+k.AccessKey)
	f.dispatched = append(f.dispatched, "KeyCreateScoped")
	return f.keyCreateErr
}
func (f *fakeProposer) ProposeKeyRevoke(ctx context.Context, accessKey string) error {
	f.calls = append(f.calls, "KeyRevoke:"+accessKey)
	f.dispatched = append(f.dispatched, "KeyRevoke")
	return nil
}

func (f *fakeProposer) ProposeBucketUpstreamPut(_ context.Context, u BucketUpstream) error {
	f.bucketUpstreamPuts = append(f.bucketUpstreamPuts, u)
	f.dispatched = append(f.dispatched, "BucketUpstreamPut")
	return f.bucketUpstreamPutErr
}

func (f *fakeProposer) ProposeBucketUpstreamDelete(_ context.Context, bucket string) error {
	f.bucketUpstreamDeletes = append(f.bucketUpstreamDeletes, bucket)
	f.dispatched = append(f.dispatched, "BucketUpstreamDelete")
	return f.bucketUpstreamDeleteErr
}

func (f *fakeProposer) ProposeBucketUpstreamCutover(_ context.Context, bucket string) error {
	f.calls = append(f.calls, "BucketUpstreamCutover:"+bucket)
	f.bucketUpstreamCutovers = append(f.bucketUpstreamCutovers, bucket)
	return f.bucketUpstreamCutoverErr
}

func (f *fakeProposer) ProposePolicyAttachToSAPut(_ context.Context, saID, policy string) error {
	f.calls = append(f.calls, "PolicyAttachToSAPut:"+saID+":"+policy)
	f.dispatched = append(f.dispatched, "PolicyAttachToSAPut")
	return nil
}

func (f *fakeProposer) ProposePolicyAttachToSADelete(_ context.Context, saID, policy string) error {
	f.calls = append(f.calls, "PolicyAttachToSADelete:"+saID+":"+policy)
	f.dispatched = append(f.dispatched, "PolicyAttachToSADelete")
	return nil
}

func (f *fakeProposer) ProposeCreateBucketWithPolicyAttach(_ context.Context, bucket, sa, policy string) error {
	f.calls = append(f.calls, "CreateBucketWithPolicyAttach:"+bucket+":"+sa+":"+policy)
	f.dispatched = append(f.dispatched, "CreateBucketWithPolicyAttach")
	return nil
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
