# Same Data, Multiple Protocols

S3, NFSv4, 9P, NBD, and Iceberg do not have identical semantics. GrainFS uses shared backend contracts where possible, then documents the differences per protocol.

The important operating rule: do not assume a feature is supported across every interface just because one protocol supports it. Check the compatibility matrix for the protocol you expose.
