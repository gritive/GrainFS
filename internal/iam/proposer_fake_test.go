package iam

import (
	"context"
)

// fakeProposer captures each Propose* call as a string so tests can
// assert the dispatch sequence without spinning up Raft. Shared across
// admin_api_test.go and other iam package tests; lifted here when
// bootstrap_test.go was removed in the IAM-only-auth migration.
type fakeProposer struct {
	calls          []string
	authEnableErr  error // legacy: kept for admin_api_test.go references; removed in Task 6
	saCreateErr    error // if non-nil, ProposeSACreate returns this
	keyCreateErr   error // if non-nil, ProposeKeyCreate returns this
	wildcardPutErr error // if non-nil, ProposeGrantWildcardPut returns this
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
	return f.saCreateErr
}
func (f *fakeProposer) ProposeSADelete(ctx context.Context, saID string) error {
	f.calls = append(f.calls, "SADelete:"+saID)
	return nil
}
func (f *fakeProposer) ProposeKeyCreate(ctx context.Context, k AccessKey) error {
	f.calls = append(f.calls, "KeyCreate:"+k.AccessKey)
	return f.keyCreateErr
}
func (f *fakeProposer) ProposeKeyCreateScoped(ctx context.Context, k AccessKey) error {
	f.calls = append(f.calls, "KeyCreateScoped:"+k.AccessKey)
	return f.keyCreateErr
}
func (f *fakeProposer) ProposeKeyRevoke(ctx context.Context, accessKey string) error {
	f.calls = append(f.calls, "KeyRevoke:"+accessKey)
	return nil
}
func (f *fakeProposer) ProposeGrantPut(ctx context.Context, g Grant) error {
	f.calls = append(f.calls, "GrantPut:"+g.SAID+":"+g.Bucket)
	return nil
}
func (f *fakeProposer) ProposeGrantDelete(ctx context.Context, saID, bucket string) error {
	f.calls = append(f.calls, "GrantDelete:"+saID+":"+bucket)
	return nil
}
func (f *fakeProposer) ProposeGrantWildcardPut(ctx context.Context, g Grant) error {
	f.calls = append(f.calls, "GrantWildcard:"+g.SAID)
	return f.wildcardPutErr
}
func (f *fakeProposer) ProposeGrantWildcardDelete(ctx context.Context, saID string) error {
	f.calls = append(f.calls, "GrantWildcardDelete:"+saID)
	return nil
}
func (f *fakeProposer) ProposeInitFirstSA(ctx context.Context, sa ServiceAccount, k AccessKey, g Grant) error {
	f.calls = append(f.calls, "InitFirstSA:"+sa.ID)
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
