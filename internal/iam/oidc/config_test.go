package oidc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseIssuerConfigsValidatesAndDefaults(t *testing.T) {
	raw := []byte(`[
		{
			"name":"example",
			"issuer_url":"https://idp.example.com/",
			"audience":"grainfs",
			"jwks_url":"https://idp.example.com/.well-known/jwks.json",
			"groups_claim":"groups",
			"group_prefix":"oidc:example:"
		}
	]`)

	cfgs, err := ParseIssuerConfigs(raw)
	require.NoError(t, err)
	require.Len(t, cfgs, 1)
	require.Equal(t, "example", cfgs[0].Name)
	require.Equal(t, FailurePolicyStrict, cfgs[0].JWKSFailurePolicy)
	require.Zero(t, cfgs[0].GraceTTL)
}

func TestParseIssuerConfigsGraceRequiresPositiveTTL(t *testing.T) {
	raw := []byte(`[
		{
			"name":"example",
			"issuer_url":"https://idp.example.com/",
			"audience":"grainfs",
			"jwks_url":"https://idp.example.com/.well-known/jwks.json",
			"groups_claim":"groups",
			"group_prefix":"oidc:example:",
			"jwks_failure_policy":"grace"
		}
	]`)

	_, err := ParseIssuerConfigs(raw)
	require.ErrorContains(t, err, "grace_ttl_seconds")
}

func TestParseIssuerConfigsRejectsDuplicateNames(t *testing.T) {
	raw := []byte(`[
		{"name":"example","issuer_url":"https://idp.example.com/","audience":"grainfs","jwks_url":"https://idp.example.com/jwks.json","groups_claim":"groups","group_prefix":"oidc:example:"},
		{"name":"example","issuer_url":"https://idp2.example.com/","audience":"grainfs","jwks_url":"https://idp2.example.com/jwks.json","groups_claim":"groups","group_prefix":"oidc:example2:"}
	]`)

	_, err := ParseIssuerConfigs(raw)
	require.ErrorContains(t, err, "duplicate")
}

func TestParseIssuerConfigsRejectsDuplicateGroupPrefixes(t *testing.T) {
	raw := []byte(`[
		{"name":"example","issuer_url":"https://idp.example.com/","audience":"grainfs","jwks_url":"https://idp.example.com/jwks.json","groups_claim":"groups","group_prefix":"oidc:shared:"},
		{"name":"example2","issuer_url":"https://idp2.example.com/","audience":"grainfs","jwks_url":"https://idp2.example.com/jwks.json","groups_claim":"groups","group_prefix":"oidc:shared:"}
	]`)

	_, err := ParseIssuerConfigs(raw)
	require.ErrorContains(t, err, "duplicate group_prefix")
}

func TestMapGroupsRejectsInjectionValues(t *testing.T) {
	cfg := IssuerConfig{Name: "example", GroupPrefix: "oidc:example:"}

	for _, value := range []string{"", "../admin", "data/eng", "data eng", "data\\eng"} {
		_, err := MapGroups(cfg, []string{value})
		require.Error(t, err, value)
	}
}

func TestMapGroupsPrefixesAndCopies(t *testing.T) {
	cfg := IssuerConfig{Name: "example", GroupPrefix: "oidc:example:"}
	in := []string{"data-eng", "ops"}

	got, err := MapGroups(cfg, in)
	require.NoError(t, err)
	require.Equal(t, []string{"oidc:example:data-eng", "oidc:example:ops"}, got)

	in[0] = "mutated"
	require.Equal(t, []string{"oidc:example:data-eng", "oidc:example:ops"}, got)
}

func TestIssuerConfigValidateRejectsHTTPURLs(t *testing.T) {
	cfg := IssuerConfig{
		Name:              "example",
		IssuerURL:         "http://idp.example.com/",
		Audience:          "grainfs",
		JWKSURL:           "https://idp.example.com/jwks.json",
		GroupsClaim:       "groups",
		GroupPrefix:       "oidc:example:",
		JWKSFailurePolicy: FailurePolicyStrict,
		GraceTTL:          time.Minute,
	}

	require.ErrorContains(t, cfg.Validate(), "https")
}
