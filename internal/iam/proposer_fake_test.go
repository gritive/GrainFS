package iam

import (
	"context"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// fakeProposer captures each Propose* call as a string so tests can
// assert the dispatch sequence without spinning up Raft. Shared across
// admin_api_test.go and other iam package tests; lifted here when
// bootstrap_test.go was removed in the IAM-only-auth migration.
//
// Two parallel logs:
//   - calls:      "<type>:<id>" — used by legacy callers (calledKeyCreate etc).
//   - dispatched: bare type names — used by HandleSACreate dispatch tests
//     to assert the empty-vs-non-empty branch chose the right cmd type.
//
// initBlocked simulates an FSM idempotent-skip on ProposeInitFirstSA: the
// propose returns nil but no records are applied, mirroring the real path
// where a concurrent caller already committed DefaultSAID. Used to exercise
// the handler's race-detect 409 branch.
//
// store/enc are optional. When store is set, Propose* methods that need
// to make their side-effects visible to the handler's post-propose lookup
// (e.g., LookupKey after InitFirstSA) apply records onto it directly.
type fakeProposer struct {
	store          *Store
	enc            *encrypt.Encryptor
	calls          []string
	dispatched     []string
	initBlocked    bool
	raceWinnerKey  AccessKey // optional: applied to store when initBlocked is true
	saCreateErr    error     // if non-nil, ProposeSACreate returns this
	keyCreateErr   error     // if non-nil, ProposeKeyCreate returns this
	wildcardPutErr error     // if non-nil, ProposeGrantWildcardPut returns this

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

func (f *fakeProposer) calledGrantPut(saID, bucket string) bool {
	for _, c := range f.calls {
		if c == "GrantPut:"+saID+":"+bucket {
			return true
		}
	}
	return false
}

func (f *fakeProposer) calledGrantDelete(saID, bucket string) bool {
	for _, c := range f.calls {
		if c == "GrantDelete:"+saID+":"+bucket {
			return true
		}
	}
	return false
}

func (f *fakeProposer) calledGrantWildcardDelete(saID string) bool {
	for _, c := range f.calls {
		if c == "GrantWildcardDelete:"+saID {
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
func (f *fakeProposer) ProposeGrantPut(ctx context.Context, g Grant) error {
	f.calls = append(f.calls, "GrantPut:"+g.SAID+":"+g.Bucket)
	f.dispatched = append(f.dispatched, "GrantPut")
	return nil
}
func (f *fakeProposer) ProposeGrantDelete(ctx context.Context, saID, bucket string) error {
	f.calls = append(f.calls, "GrantDelete:"+saID+":"+bucket)
	f.dispatched = append(f.dispatched, "GrantDelete")
	return nil
}
func (f *fakeProposer) ProposeGrantWildcardPut(ctx context.Context, g Grant) error {
	f.calls = append(f.calls, "GrantWildcard:"+g.SAID)
	f.dispatched = append(f.dispatched, "GrantWildcardPut")
	return f.wildcardPutErr
}
func (f *fakeProposer) ProposeGrantWildcardDelete(ctx context.Context, saID string) error {
	f.calls = append(f.calls, "GrantWildcardDelete:"+saID)
	f.dispatched = append(f.dispatched, "GrantWildcardDelete")
	return nil
}
func (f *fakeProposer) ProposeInitFirstSA(ctx context.Context, sa ServiceAccount, k AccessKey, g Grant) error {
	f.calls = append(f.calls, "InitFirstSA:"+sa.ID)
	f.dispatched = append(f.dispatched, "InitFirstSA")
	if f.initBlocked {
		// Simulate FSM idempotent skip: a concurrent caller already won
		// the race and committed DefaultSAID with their own key. Apply the
		// winner's records (so post-propose LookupKey for OUR access_key
		// fails) and return nil — mirroring real FSM behavior where the
		// loser's payload is silently dropped.
		if f.store != nil && f.raceWinnerKey.AccessKey != "" {
			f.store.applySACreate(ServiceAccount{ID: DefaultSAID, Name: "winner"})
			f.store.applyKeyCreate(f.raceWinnerKey)
		}
		return nil
	}
	if f.store != nil {
		f.store.applySACreate(sa)
		f.store.applyKeyCreate(k)
		f.store.applyGrantWildcardPut(g)
	}
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
