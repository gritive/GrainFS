package serveruntime

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
)

func TestWireIAMPolicyStores_SeedsBuiltinsAndInjects(t *testing.T) {
	fsm := cluster.NewMetaFSM()
	s, err := WireIAMPolicyStores(context.Background(), fsm, 0 /* default TTL */)
	require.NoError(t, err)
	require.NotNil(t, s)
	require.NotNil(t, s.Policies)
	require.NotNil(t, s.Resolver)
	require.NotNil(t, s.Adapter)
	// All four built-ins seeded with builtin=true.
	for _, name := range []string{"readonly", "readwrite", "writeonly", "bucket-admin"} {
		assert.True(t, s.Policies.IsBuiltin(name), "builtin %q not seeded as builtin", name)
	}
}

func TestWireIAMPolicyStores_NilFSM(t *testing.T) {
	// Nil FSM is allowed (e.g. unit tests that only need the bundle).
	s, err := WireIAMPolicyStores(context.Background(), nil, 0)
	require.NoError(t, err)
	require.NotNil(t, s)
}

// TestWireIAMPolicyStores_InjectsAllFiveStoresIntoFSM verifies that
// WireIAMPolicyStores actually calls the FSM's Set*Store injectors with the
// same instances it returns in the bundle. Without this test, removing every
// fsm.Set*() call inside WireIAMPolicyStores would still make the previous
// non-nil assertions pass — the bundle would have populated stores that the
// FSM apply path never sees, and §2/§3 MetaCmds would silently no-op.
//
// We verify each of the five injections by applying a MetaCmd of the
// corresponding type through the FSM and asserting the bundle's store reflects
// the mutation (or, for the resolver, that its cache responds to invalidation).
func TestWireIAMPolicyStores_InjectsAllFiveStoresIntoFSM(t *testing.T) {
	ctx := context.Background()
	fsm := cluster.NewMetaFSM()
	s, err := WireIAMPolicyStores(ctx, fsm, 0)
	require.NoError(t, err)

	// 1) PolicyStore injection: PolicyPut → bundle.Policies sees the doc.
	doc := []byte(`{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`)
	policyPayload, err := cluster.EncodePolicyPutPayload("probe-policy", doc, false)
	require.NoError(t, err)
	policyCmd, err := cluster.EncodeMetaCmdForTest(cluster.MetaCmdTypePolicyPut, policyPayload)
	require.NoError(t, err)
	require.NoError(t, fsm.ApplyCmdForTest(policyCmd))
	got, err := s.Policies.GetRaw(ctx, "probe-policy")
	require.NoError(t, err, "policyStore not injected: FSM apply did not land in bundle.Policies")
	assert.Equal(t, doc, got)

	// 2) GroupStore injection: GroupPut → bundle.Groups sees the group.
	groupPayload, err := cluster.EncodeGroupPutPayload("probe-group", []string{"probe-policy"})
	require.NoError(t, err)
	groupCmd, err := cluster.EncodeMetaCmdForTest(cluster.MetaCmdTypeGroupPut, groupPayload)
	require.NoError(t, err)
	require.NoError(t, fsm.ApplyCmdForTest(groupCmd))
	attached, err := s.Groups.AttachedPolicies(ctx, "probe-group")
	require.NoError(t, err, "groupStore not injected")
	assert.Equal(t, []string{"probe-policy"}, attached)

	// 3) PolicyAttachStore injection: PolicyAttachToSAPut → bundle.Attach sees the link.
	attachPayload, err := cluster.EncodePolicyAttachToSAPutPayload("probe-sa", "probe-policy")
	require.NoError(t, err)
	attachCmd, err := cluster.EncodeMetaCmdForTest(cluster.MetaCmdTypePolicyAttachToSAPut, attachPayload)
	require.NoError(t, err)
	require.NoError(t, fsm.ApplyCmdForTest(attachCmd))
	pols, err := s.Attach.SAPolicies(ctx, "probe-sa")
	require.NoError(t, err, "policyAttachStore not injected")
	assert.Equal(t, []string{"probe-policy"}, pols)

	// 4) BucketPolicyStore injection: BucketPolicyPut → bundle.BucketPols sees the doc.
	bpDoc := []byte(`{"Statement":[{"Effect":"Allow","Principal":{"AWS":["probe-sa"]},"Action":"s3:GetObject","Resource":"arn:aws:s3:::probe-bucket/*"}]}`)
	bpPayload, err := cluster.EncodeBucketPolicyPutPayload("probe-bucket", bpDoc)
	require.NoError(t, err)
	bpCmd, err := cluster.EncodeMetaCmdForTest(cluster.MetaCmdTypeBucketPolicyPut, bpPayload)
	require.NoError(t, err)
	require.NoError(t, fsm.ApplyCmdForTest(bpCmd))
	gotBP, err := s.BucketPols.Get(ctx, "probe-bucket")
	require.NoError(t, err, "bucketPolicyStore not injected")
	assert.Equal(t, bpDoc, gotBP)

	// 5) Resolver injection: BucketPolicyPut above should have invalidated the
	//    resolver's cache for "probe-bucket". A subsequent HasBucketPolicy call
	//    must return true (the doc was just stored). If the FSM held a different
	//    Resolver instance, our bundle's resolver would still see "not present".
	has, err := s.Resolver.HasBucketPolicy(ctx, "probe-bucket")
	require.NoError(t, err)
	assert.True(t, has, "policyResolver not injected: invalidation did not propagate through bundle.Resolver")
}
