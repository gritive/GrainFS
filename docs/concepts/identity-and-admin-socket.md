# Identity and Admin Socket

Mutating administration starts from the local Unix domain socket, usually `<data>/admin.sock`. A fresh node can run in Phase 0 anonymous mode for local development. Creating service accounts moves the deployment toward authenticated operation.

S3 uses IAM-style credentials. NFSv4, 9P, and NBD are network-bound interfaces and must be protected by listener binding, firewall policy, private networks, or mount-specific controls.
