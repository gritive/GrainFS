// Package icebergadmin is the admin-plane client for grainfs Iceberg config operations.
// Consumed by the CLI (cmd/grainfs) and tests.
package icebergadmin

import (
	"github.com/gritive/GrainFS/internal/iamadmin"
)

// BaseOptions matches iamadmin.BaseOptions for shape parity.
type BaseOptions = iamadmin.BaseOptions

// IcebergConfigOptions holds all parameters for RunIcebergConfig.
type IcebergConfigOptions struct {
	BaseOptions
	Warehouse string
	SAID      string
	NoReveal  bool
}
