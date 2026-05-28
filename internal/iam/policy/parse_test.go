package policy

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParse_AcceptsMinimal(t *testing.T) {
	doc := []byte(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::a/*"}]}`)
	p, err := Parse(doc)
	require.NoError(t, err)
	require.Len(t, p.Statement, 1)
	require.Equal(t, EffectAllow, p.Statement[0].Effect)
}

func TestParse_AcceptsAbsentVersion(t *testing.T) {
	doc := []byte(`{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`)
	_, err := Parse(doc)
	require.NoError(t, err)
}

func TestParse_RejectsNotAction(t *testing.T) {
	doc := []byte(`{"Statement":[{"Effect":"Allow","NotAction":"s3:GetObject","Resource":"*"}]}`)
	_, err := Parse(doc)
	require.Error(t, err)
	require.NotEmpty(t, err.Error())
}

func TestParse_RejectsUnsupportedConditionKey(t *testing.T) {
	doc := []byte(`{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*","Condition":{"StringEquals":{"aws:UserAgent":"x"}}}]}`)
	_, err := Parse(doc)
	require.Error(t, err)
}

func TestParse_RejectsMalformedARN(t *testing.T) {
	doc := []byte(`{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"notanarn"}]}`)
	_, err := Parse(doc)
	require.Error(t, err)
}

func TestParse_AcceptsBothCondKeys(t *testing.T) {
	doc := []byte(`{"Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::a","Condition":{"IpAddress":{"aws:SourceIp":"10.0.0.0/8"},"StringLike":{"s3:prefix":"logs/*"}}}]}`)
	_, err := Parse(doc)
	require.NoError(t, err)
}

func TestParse_AcceptsIAMGroupAdminResource(t *testing.T) {
	doc := []byte(`{"Statement":[{"Effect":"Allow","Action":"grainfs:IAMGroupPolicyAttach","Resource":"iam/group/oidc:example:admins"}]}`)
	_, err := Parse(doc)
	require.NoError(t, err)
}

func TestParse_AcceptsIAMPolicyAttachAdminResource(t *testing.T) {
	doc := []byte(`{"Statement":[{"Effect":"Allow","Action":"grainfs:IAMPolicyAttach","Resource":"iam/policy/storage-admin/attach/sa/sa-app"}]}`)
	_, err := Parse(doc)
	require.NoError(t, err)
}

func TestParse_AcceptsIAMGroupPolicyAdminResource(t *testing.T) {
	doc := []byte(`{"Statement":[{"Effect":"Allow","Action":"grainfs:IAMGroupPolicyAttach","Resource":"iam/group/storage-admins/policy/storage-admin"}]}`)
	_, err := Parse(doc)
	require.NoError(t, err)
}

func TestParse_AcceptsRemainingAdminRouteResources(t *testing.T) {
	for _, resource := range []string{
		"iam/mount-sa/*",
		"iam/mount-sa/alice-mount/policy/NFSMountOnly",
		"iam/upstream/*",
		"iam/upstream/logs",
		"iam/upstream/*/cutover",
		"admin/config/*",
		"admin/config/oidc.enabled",
		"admin/dashboard/token",
		"admin/dashboard/token/rotate",
	} {
		t.Run(resource, func(t *testing.T) {
			doc := []byte(`{"Statement":[{"Effect":"Allow","Action":"grainfs:Admin*","Resource":"` + resource + `"}]}`)
			_, err := Parse(doc)
			require.NoError(t, err)
		})
	}
}
