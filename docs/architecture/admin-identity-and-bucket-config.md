# Admin, Identity, and Bucket Configuration

`GrainFS` separates operator control paths from public data-plane traffic. The S3
API remains the object data-plane surface. Administrative operations use the
admin Unix socket unless they are part of the browser dashboard.

## Admin Surfaces

- CLI administration uses `<data-dir>/admin.sock` with filesystem permissions.
  `GrainFS` creates the socket with mode `0660`; multi-operator deployments should
  use `--admin-group` to grant access through a shared group.
- Dashboard traffic uses HTTP routes. A browser cannot rely on a local Unix
  socket, so dashboard access stays on HTTP. Configure dashboard authentication
  before exposing it outside localhost.
- Cluster key rotation keeps a separate owner-only socket,
  `<data-dir>/rotate.sock`, because rotation handles PSK material. That socket
  is intentionally narrower than the shared admin socket.

## IAM-Only Bootstrap

`GrainFS` does not accept long-lived `--access-key` or `--secret-key` server
flags. Operators create the first service account through admin UDS:

```bash
grainfs iam sa create admin --endpoint <data-dir>/admin.sock
```

S3 clients use the returned one-time secret for SigV4 traffic. This keeps
bootstrap credentials out of process arguments. Service accounts, access keys,
and grants form the single authority for S3 authentication.

## Bucket-Scoped Upstream Credentials

Pull-through migration credentials are bucket configuration, not process
configuration. Operators register them through the bucket CLI:

```bash
grainfs bucket upstream put <bucket> \
  --endpoint <data-dir>/admin.sock \
  --endpoint-url http://source.example:9000 \
  --access-key <access-key> \
  --secret-key <secret-key>
```

The bucket surface reflects the ownership model: operators attach upstream
credentials to a bucket resource. `GrainFS` stores the encrypted credential
payload in the IAM-backed metadata store. The CLI path stays under
`grainfs bucket upstream ...`.

Bucket-upstream records use persisted metadata commands. During rolling
upgrades, configure upstreams only after every node supports those commands.
Older followers may ignore an unknown metadata command until they later catch up
through snapshot install.

## Bucket Lifecycle Replication

Lifecycle policy changes are cluster metadata. `PUT` and `DELETE` on
`/{bucket}?lifecycle` replicate through the metadata Raft FSM before workers use
them. `GrainFS` stores the original S3 lifecycle XML body so `GET ?lifecycle` can
round-trip the operator's policy bytes.

The lifecycle worker still runs only on the leader. Replicating the policy means
a later leader sees the same policy and can continue lifecycle execution without
depending on the previous leader's local BadgerDB contents.
