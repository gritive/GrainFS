# CLI Reference

The `grainfs` binary is the main operator and developer interface.

Common command families:

| Command | Purpose |
| --- | --- |
| `grainfs serve` | Start a local node or cluster node. |
| `grainfs status` | Inspect node or cluster status. |
| `grainfs dashboard` | Open or inspect dashboard state. |
| `grainfs doctor` | Run diagnostics. |
| `grainfs bucket` | Manage buckets, policies, versioning, and upstreams. |
| `grainfs iam` | Manage service accounts, policies, groups, and mount identities. |
| `grainfs cluster` | Join, observe, configure, and inspect clusters. |
| `grainfs nfs` | Manage NFS exports and debug helpers. |
| `grainfs volume` | Manage NBD volumes. |
| `grainfs iceberg` | Generate or inspect Iceberg client configuration. |
| `grainfs audit` | Work with audit log workflows. |
| `grainfs recover` | Plan or run recovery operations. |
| `grainfs migrate` | Run migration workflows. |
| `grainfs scrub` | Run scrubber workflows. |

Use `grainfs <command> --help` for exact flags.
