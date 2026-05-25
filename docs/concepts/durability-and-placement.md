# Durability and Placement

GrainFS uses cluster membership, placement, Raft-backed coordination, and erasure coding to decide where data should live. Operators do not choose a fixed `k/m` profile at startup in the zero-config EC model. GrainFS derives the target from cluster state and refuses writes when required durability targets are not writable.

See [architecture durability and placement](../architecture/durability-and-placement.md) for internal flow.
