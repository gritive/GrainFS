# Architecture Overview

GrainFS is organized as a single binary with layered internal packages. The public interfaces are S3, NFSv4, 9P, NBD, Iceberg REST Catalog, CLI, Web UI, and admin APIs.

The main internal layers are storage, metadata, server execution, protocol adapters, transport, cluster coordination, lifecycle, metrics, and recovery.

Use this section for current system design. Internal ADRs, planning notes, and agent-generated work logs are not part of the customer documentation set.
