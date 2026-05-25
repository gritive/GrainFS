# Deployment Checklist

Before exposing GrainFS beyond a local development machine, check:

- Authentication is enabled where required.
- NFSv4, 9P, and NBD listeners are bound to trusted networks.
- TLS and proxy-trust settings match the deployment.
- Metrics and incident endpoints are monitored.
- Recovery commands have been tested in a drill.

Use [Production runbook](production-runbook.md) for the full workflow.
