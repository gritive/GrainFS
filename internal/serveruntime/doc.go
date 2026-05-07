// Package serveruntime hosts the cobra-free server runtime for the grainfs
// serve command. The cluster bootstrap path lives entirely here as
// Run(ctx, Config); cmd/grainfs is reduced to flag registration plus a thin
// runServe that builds Config and dispatches to Run.
package serveruntime
