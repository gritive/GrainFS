# Namespace and Buckets

Buckets are the shared unit that S3 clients see as buckets and file protocols expose as mountable roots. Object keys map to paths where the protocol supports a path view. NBD volumes are managed as block-oriented resources and should be treated separately from file/object buckets.

Use compatibility references for protocol-specific edge cases.
