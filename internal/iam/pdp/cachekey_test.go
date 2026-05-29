package pdp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCacheKey(t *testing.T) {
	base := Request{
		SchemaVersion: 1,
		RequestID:     "req-1",
		Principal: WirePrincipal{
			Kind: "sa", ID: "alice", Issuer: "iss", Subject: "sub", CredentialID: "cred",
			Groups: []string{"g2", "g1"},
		},
		Action:   "s3:GetObject",
		Resource: "arn:bucket/obj",
		Protocol: "s3",
		Context:  map[string]string{"ip": "1.2.3.4", "tls": "1.3"},
	}

	t.Run("identical except request_id ⇒ same key", func(t *testing.T) {
		other := base
		other.RequestID = "req-2"
		require.Equal(t, cacheKey(base), cacheKey(other))
	})

	t.Run("differing schema_version ⇒ different key", func(t *testing.T) {
		other := base
		other.SchemaVersion = 2
		require.NotEqual(t, cacheKey(base), cacheKey(other))
	})

	t.Run("nil groups vs empty groups ⇒ same key", func(t *testing.T) {
		a := base
		a.Principal.Groups = nil
		b := base
		b.Principal.Groups = []string{}
		require.Equal(t, cacheKey(a), cacheKey(b))
	})

	t.Run("group reorder ⇒ same key", func(t *testing.T) {
		other := base
		other.Principal.Groups = []string{"g1", "g2"}
		require.Equal(t, cacheKey(base), cacheKey(other))
	})

	t.Run("group set difference ⇒ different key", func(t *testing.T) {
		other := base
		other.Principal.Groups = []string{"g1", "g3"}
		require.NotEqual(t, cacheKey(base), cacheKey(other))
	})

	t.Run("differing action ⇒ different key", func(t *testing.T) {
		other := base
		other.Action = "s3:PutObject"
		require.NotEqual(t, cacheKey(base), cacheKey(other))
	})

	t.Run("differing resource ⇒ different key", func(t *testing.T) {
		other := base
		other.Resource = "arn:bucket/other"
		require.NotEqual(t, cacheKey(base), cacheKey(other))
	})

	t.Run("differing context value ⇒ different key", func(t *testing.T) {
		other := base
		other.Context = map[string]string{"ip": "9.9.9.9", "tls": "1.3"}
		require.NotEqual(t, cacheKey(base), cacheKey(other))
	})

	t.Run("nil context vs empty context ⇒ same key", func(t *testing.T) {
		a := base
		a.Context = nil
		b := base
		b.Context = map[string]string{}
		require.Equal(t, cacheKey(a), cacheKey(b))
	})

	t.Run("absent vs empty-string context value ⇒ same key", func(t *testing.T) {
		a := base
		a.Context = map[string]string{"ip": "1.2.3.4", "tls": "1.3"}
		b := base
		b.Context = map[string]string{"ip": "1.2.3.4", "tls": "1.3", "extra": ""}
		require.Equal(t, cacheKey(a), cacheKey(b))
	})
}
