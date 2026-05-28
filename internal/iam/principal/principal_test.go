package principal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConstructorsProduceStableKindsAndIDs(t *testing.T) {
	require.Equal(t, Principal{Kind: KindServiceAccount, ID: "sa-app", Source: "iam"}, ServiceAccount("sa-app"))
	require.Equal(t, Principal{Kind: KindMountSA, ID: "alice-mount", Source: "mount-sa"}, MountSA("alice-mount"))
	require.Equal(t, Principal{Kind: KindProtocolCredential, ID: "sa-app", Source: "protocol-credential", CredentialID: "pc_123"}, ProtocolCredential("sa-app", "pc_123"))
}

func TestOIDCCopiesGroups(t *testing.T) {
	groups := []string{"oidc:example:data-eng"}
	p := OIDC("https://idp.example.com/", "user-1", "oidc:abc:user-1", groups)
	groups[0] = "mutated"

	require.Equal(t, KindOIDC, p.Kind)
	require.Equal(t, "oidc:abc:user-1", p.ID)
	require.Equal(t, "https://idp.example.com/", p.Issuer)
	require.Equal(t, "user-1", p.Subject)
	require.Equal(t, []string{"oidc:example:data-eng"}, p.Groups)

	copied := p.GroupNames()
	copied[0] = "changed"
	require.Equal(t, []string{"oidc:example:data-eng"}, p.GroupNames())
}
