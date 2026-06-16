# TODO

## Follow-ups

- **[P2] Cluster object read-failover after data-group leader death.** When the object's data-raft
  group leader is killed, `GetObject` → `forwardRuntime.readObject` → `SendReadStream` forwards to
  the dead leader / a stale leader-hint and returns 500 `forward: no reachable peer` instead of
  failing over to a surviving peer or EC-reconstructing from the k surviving shards. Live followers
  reply "not-leader" hinting the dead node (`internal/cluster/forward_sender.go:542-554`); no
  EC-reconstruct fallback. Reproduce: `AppendObject Cluster4Node` › `survives owner kill after
  coalesce` (`tests/e2e/append_object_test.go:291`). Surfaced by the gen-0 placement-routing fix
  (routing now correctly targets the dead leader's group); distinct subsystem from placement routing.
