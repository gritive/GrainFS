//go:build !unix

package resourcewatch

import (
	"context"
	"errors"
)

var ErrFDProviderUnsupported = errors.New("resourcewatch: fd provider unsupported on this platform")

type FDProviderOptions struct {
	Dir               string
	ClassificationCap int
}

type ProcessFDProvider struct{}

func NewFDProvider(opts FDProviderOptions) *ProcessFDProvider {
	return &ProcessFDProvider{}
}

func (p *ProcessFDProvider) Snapshot(ctx context.Context) (FDSnapshot, error) {
	return FDSnapshot{}, ErrFDProviderUnsupported
}
