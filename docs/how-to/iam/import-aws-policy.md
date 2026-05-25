# Pasting AWS IAM Policies into GrainFS

GrainFS accepts a subset of AWS IAM JSON policy syntax. This page lists what works, what doesn't, and how to validate before applying.

## Validate before apply

```bash
grainfs iam policy validate --file my-policy.json
```

Exits 0 if accepted; non-zero with a specific error pointing at the rejected field.

## Supported

- `Version`: `"2012-10-17"` or absent
- `Statement`: array
- `Effect`: `"Allow"` / `"Deny"`
- `Action`: string or array. Wildcards: `s3:*`, `s3:Get*`, `iceberg:*`, `iceberg:*Table`
- `Resource`: string or array. ARN scheme `arn:aws:s3:::<bucket>[/<key-or-prefix-with-*>]`. `*` matches all buckets.
- `Sid`: optional, used in audit logs / `policy simulate` output
- `Principal` (bucket policies only): `{"AWS": ["<sa_id>"]}` or `"*"` (only honored if `iam.allow-anonymous-bucket-policy=true`)
- `Condition`:
  - `IpAddress` / `NotIpAddress` on `aws:SourceIp` (CIDR notation)
  - `StringEquals` / `StringLike` on `s3:prefix` (list operations only)

## Unsupported (rejected at policy-put)

- `NotAction`, `NotResource`, `NotPrincipal`
- Any condition key other than `aws:SourceIp`, `s3:prefix`
- Policy variables (`${aws:username}`)
- Inline policies on SAs (attach by name instead)
- Cross-account `Principal` ARNs

## Examples (paste-able)

### 1. Read-only on one bucket

```json
{"Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:ListBucket"],"Resource":["arn:aws:s3:::analytics","arn:aws:s3:::analytics/*"]}]}
```

### 2. Write a single prefix

```json
{"Statement":[{"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::raw/incoming/*"}]}
```

### 3. Read with IP allowlist

```json
{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::analytics/*","Condition":{"IpAddress":{"aws:SourceIp":["10.0.0.0/8","192.168.0.0/16"]}}}]}
```

### 4. Iceberg readwrite on one warehouse

```json
{"Statement":[{"Effect":"Allow","Action":["iceberg:*"],"Resource":"arn:aws:s3:::analytics"}]}
```

### 5. Deny override

```json
{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"},{"Effect":"Deny","Action":"s3:DeleteObject","Resource":"arn:aws:s3:::archive/*"}]}
```

### 6. Bucket policy granting one SA

```json
{"Statement":[{"Effect":"Allow","Principal":{"AWS":["sa-12345"]},"Action":["s3:GetObject","s3:PutObject"],"Resource":"arn:aws:s3:::shared/*"}]}
```

### 7. List with prefix

```json
{"Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::reports","Condition":{"StringLike":{"s3:prefix":["2026/*"]}}}]}
```
