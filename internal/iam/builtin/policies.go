// Package builtin holds the four managed policy documents seeded at cluster
// bootstrap. Names: readonly, readwrite, writeonly, bucket-admin. Per spec
// §"IAM Model / Built-in managed policies". bucket-admin intentionally
// excludes s3:CreateBucket / s3:DeleteBucket / s3:PutBucketPolicy /
// s3:DeleteBucketPolicy (Decision #8: admin-UDS only).
package builtin

import (
	"context"

	"github.com/gritive/GrainFS/internal/iam/policystore"
)

var builtinDocs = map[string][]byte{
	"readonly":     []byte(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:HeadObject","s3:ListBucket","s3:GetBucketLocation","s3:GetObjectTagging","iceberg:GetCatalogConfig","iceberg:ListNamespaces","iceberg:LoadNamespace","iceberg:HeadNamespace","iceberg:ListTables","iceberg:LoadTable","iceberg:HeadTable"],"Resource":"*"}]}`),
	"readwrite":    []byte(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:HeadObject","s3:ListBucket","s3:GetBucketLocation","s3:PutObject","s3:DeleteObject","s3:CopyObject","s3:AbortMultipartUpload","s3:ListMultipartUploads","s3:GetObjectTagging","s3:PutObjectTagging","s3:DeleteObjectTagging","iceberg:GetCatalogConfig","iceberg:ListNamespaces","iceberg:LoadNamespace","iceberg:HeadNamespace","iceberg:ListTables","iceberg:LoadTable","iceberg:HeadTable","iceberg:CreateNamespace","iceberg:CreateTable","iceberg:CommitTable","iceberg:DeleteTable","iceberg:DeleteNamespace","iceberg:CommitTransaction"],"Resource":"*"}]}`),
	"writeonly":    []byte(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:PutObject","s3:AbortMultipartUpload","s3:PutObjectTagging","s3:DeleteObjectTagging","iceberg:CreateTable","iceberg:CommitTable","iceberg:CommitTransaction"],"Resource":"*"}]}`),
	"bucket-admin": []byte(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:HeadObject","s3:PutObject","s3:DeleteObject","s3:CopyObject","s3:ListBucket","s3:ListMultipartUploads","s3:AbortMultipartUpload","s3:GetBucketLocation","s3:GetBucketPolicy","s3:GetObjectTagging","s3:PutObjectTagging","s3:DeleteObjectTagging","iceberg:GetCatalogConfig","iceberg:ListNamespaces","iceberg:LoadNamespace","iceberg:HeadNamespace","iceberg:CreateNamespace","iceberg:DeleteNamespace","iceberg:ListTables","iceberg:LoadTable","iceberg:HeadTable","iceberg:CreateTable","iceberg:CommitTable","iceberg:DeleteTable","iceberg:CommitTransaction"],"Resource":"*"}]}`),
}

// SeedAll writes each built-in policy with builtin=true. Idempotent: re-seeding
// an existing builtin re-writes the doc but keeps builtin=true (Put on a
// builtin name with builtin=true is allowed). Returns the first store error.
func SeedAll(ctx context.Context, ps *policystore.InMemoryStore) error {
	for name, doc := range builtinDocs {
		if err := ps.Put(ctx, name, doc, true); err != nil {
			return err
		}
	}
	return nil
}

// IsBuiltinName reports whether name is one of the four built-in policy names.
func IsBuiltinName(name string) bool {
	_, ok := builtinDocs[name]
	return ok
}
