// Package serveruntime hosts cobra-free helpers used by the grainfs serve
// command's cluster bootstrap path. It is a staging package: small,
// self-contained pieces have been peeled off cmd/grainfs/serve.go so they
// can be unit-tested in isolation. Future work moves the runCluster body
// itself into this package as a single Run(ctx, cfg) entry point.
package serveruntime
