# Security Boundaries

S3 requests use IAM-style credentials once authentication is enabled. File and block protocol listeners do not inherit S3 IAM semantics. Treat NFSv4, 9P, and NBD as network services that need private binding, firewall rules, or mount-level identity controls.

Secrets should be passed through environment variables or files, not hard-coded into scripts.
