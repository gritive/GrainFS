# IAM Explain CLI Plan

## Goal

Ship the next IAM DX follow-up: `grainfs iam explain --sa X --s3 put s3://bucket/key`.
The command should turn a request-shaped S3 expression into the same `(action,
resource)` tuple used by `grainfs iam policy simulate`, then delegate to the
existing admin policy simulation endpoint.

## Scope

- Add `grainfs iam explain`.
- Support the S3 form only in this slice:
  - `--sa <service-account-id>`
  - `--s3 <verb> <s3://bucket[/key]>`
- Map common S3 verbs to GrainFS IAM actions:
  - `get`/`head` -> `s3:GetObject`
  - `put` -> `s3:PutObject`
  - `delete`/`rm` -> `s3:DeleteObject`
  - `list`/`ls` -> `s3:ListBucket`
- Render the derived action/resource before the simulate result in text mode.
- Preserve `--json` by returning structured JSON with the derived request and
  simulate response.
- Leave OIDC and protocol-credential explain forms for later follow-ups.

## Non-Goals

- No new server endpoint.
- No local policy evaluator.
- No support for AWS condition keys or policy variables.
- No multi-command natural language parser.

## Tasks

1. Add request-shape parsing tests.
   - Verify object verbs build `arn:aws:s3:::bucket/key`.
   - Verify list builds `arn:aws:s3:::bucket`.
   - Verify invalid URI/verb errors are actionable.

2. Add command execution tests.
   - Fake admin UDS receives the expected simulate request.
   - Text output includes derived `Action`, `Resource`, and policy decision.
   - JSON output includes derived request fields and the simulate response.

3. Implement `iamadmin.RunExplain`.
   - Reuse `Client.PolicySimulate`.
   - Keep parsing helpers small and local to `internal/iamadmin`.
   - Use existing `BaseOptions` output conventions.

4. Wire `cmd/grainfs` thin runner.
   - Add `iam explain --sa <id> --s3 <verb> <s3-uri>`.
   - Keep `cmd/grainfs` logic to flag collection plus `RunExplain` call.

5. Update docs and TODO.
   - Add a concise operator/user doc entry for `iam explain`.
   - Remove the completed `grainfs iam explain` TODO sub-item.

## Verification

```bash
go test ./internal/iamadmin ./cmd/grainfs -run 'Explain|PolicySimulate' -count=1
go test ./internal/iamadmin ./cmd/grainfs -count=1
git diff --check
```
