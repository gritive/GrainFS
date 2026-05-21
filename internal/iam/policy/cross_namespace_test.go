package policy

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateMountSAPolicyAttach_RejectsS3Action(t *testing.T) {
	policyJSON := `{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`
	err := ValidateForMountSAAttach(policyJSON)
	require.Error(t, err)
	require.Contains(t, err.Error(), "s3:GetObject")
}

func TestValidateMountSAPolicyAttach_RejectsIcebergAction(t *testing.T) {
	policyJSON := `{"Statement":[{"Effect":"Allow","Action":"iceberg:CreateTable","Resource":"*"}]}`
	err := ValidateForMountSAAttach(policyJSON)
	require.Error(t, err)
	require.Contains(t, err.Error(), "iceberg:CreateTable")
}

func TestValidateMountSAPolicyAttach_AllowsNFSAction(t *testing.T) {
	policyJSON := `{"Statement":[{"Effect":"Allow","Action":"grainfs:NFSMount","Resource":"arn:aws:s3:::bucket-x"}]}`
	require.NoError(t, ValidateForMountSAAttach(policyJSON))
}

func TestValidateMountSAPolicyAttach_Allows9PAction(t *testing.T) {
	policyJSON := `{"Statement":[{"Effect":"Allow","Action":"grainfs:9PAttach","Resource":"arn:aws:s3:::bucket-x"}]}`
	require.NoError(t, ValidateForMountSAAttach(policyJSON))
}

func TestValidateMountSAPolicyAttach_RejectsWildcard(t *testing.T) {
	// "*" is ambiguous for MountSA — it could grant S3 access through a MountSA principal
	policyJSON := `{"Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	err := ValidateForMountSAAttach(policyJSON)
	require.Error(t, err)
	require.Contains(t, err.Error(), "wildcard")
}

func TestValidateMountSAPolicyAttach_RejectsMixed(t *testing.T) {
	policyJSON := `{"Statement":[{"Effect":"Allow","Action":["grainfs:NFSMount","s3:GetObject"],"Resource":"*"}]}`
	err := ValidateForMountSAAttach(policyJSON)
	require.Error(t, err)
	require.Contains(t, err.Error(), "s3:GetObject")
}

func TestValidateS3SAPolicyAttach_RejectsNFSAction(t *testing.T) {
	policyJSON := `{"Statement":[{"Effect":"Allow","Action":"grainfs:NFSMount","Resource":"*"}]}`
	err := ValidateForS3SAAttach(policyJSON)
	require.Error(t, err)
	require.Contains(t, err.Error(), "grainfs:NFSMount")
}

func TestValidateS3SAPolicyAttach_Rejects9PAction(t *testing.T) {
	policyJSON := `{"Statement":[{"Effect":"Allow","Action":"grainfs:9PAttach","Resource":"*"}]}`
	err := ValidateForS3SAAttach(policyJSON)
	require.Error(t, err)
	require.Contains(t, err.Error(), "grainfs:9PAttach")
}

func TestValidateS3SAPolicyAttach_AllowsS3Action(t *testing.T) {
	policyJSON := `{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-x/*"}]}`
	require.NoError(t, ValidateForS3SAAttach(policyJSON))
}

func TestValidateS3SAPolicyAttach_AllowsWildcard(t *testing.T) {
	// "*" on S3 SA is fine — it expands within s3/iceberg namespace only
	policyJSON := `{"Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	require.NoError(t, ValidateForS3SAAttach(policyJSON))
}

func TestValidateS3SAPolicyAttach_AllowsIcebergAction(t *testing.T) {
	policyJSON := `{"Statement":[{"Effect":"Allow","Action":"iceberg:CreateTable","Resource":"*"}]}`
	require.NoError(t, ValidateForS3SAAttach(policyJSON))
}

func TestParse_AcceptsGrainfsAction(t *testing.T) {
	doc := []byte(`{"Statement":[{"Effect":"Allow","Action":"grainfs:NFSMount","Resource":"arn:aws:s3:::bucket-x"}]}`)
	_, err := Parse(doc)
	require.NoError(t, err, "Parse should accept grainfs: namespace actions")
}

func TestParse_Accepts9PAttachAction(t *testing.T) {
	doc := []byte(`{"Statement":[{"Effect":"Allow","Action":"grainfs:9PAttach","Resource":"arn:aws:s3:::bucket-x"}]}`)
	_, err := Parse(doc)
	require.NoError(t, err, "Parse should accept grainfs:9PAttach action")
}
