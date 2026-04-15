//go:build !linux

package main

import "github.com/gritive/GrainFS/internal/volume"

type nbdServerStub struct{}

func (s *nbdServerStub) Close() error { return nil }

func startNBDServer(_ *volume.Manager, _ string, _ int) (*nbdServerStub, error) {
	return nil, nil
}
