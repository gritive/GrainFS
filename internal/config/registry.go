package config

import (
	"context"
	"fmt"
	"strconv"
)

// Spec describes a single config key: its type, default, validation, and reload callback.
type Spec interface {
	defaultStr() string
	validate(string) error
	fireReload(context.Context, string) error
	kind() string
}

// BoolSpec is a typed spec for boolean config keys.
type BoolSpec struct {
	Default  bool
	OnReload func(context.Context, bool) error
}

func (s BoolSpec) defaultStr() string {
	if s.Default {
		return "true"
	}
	return "false"
}

func (s BoolSpec) validate(v string) error {
	if v != "true" && v != "false" {
		return fmt.Errorf("%w: bool key requires \"true\" or \"false\", got %q", ErrInvalidValue, v)
	}
	return nil
}

func (s BoolSpec) fireReload(ctx context.Context, v string) error {
	if s.OnReload == nil {
		return nil
	}
	b := v == "true"
	return s.OnReload(ctx, b)
}

func (s BoolSpec) kind() string { return "bool" }

// StringSpec is a typed spec for string config keys.
type StringSpec struct {
	Default  string
	OnReload func(context.Context, string) error
}

func (s StringSpec) defaultStr() string { return s.Default }

func (s StringSpec) validate(_ string) error { return nil }

func (s StringSpec) fireReload(ctx context.Context, v string) error {
	if s.OnReload == nil {
		return nil
	}
	return s.OnReload(ctx, v)
}

func (s StringSpec) kind() string { return "string" }

// TriggerSpec is a typed spec for trigger (one-shot action) config keys.
// The only valid value is "now".
type TriggerSpec struct {
	OnTrigger func(context.Context) error
}

func (s TriggerSpec) defaultStr() string { return "" }

func (s TriggerSpec) validate(v string) error {
	if v != "now" {
		return fmt.Errorf("%w: trigger key requires \"now\", got %q", ErrInvalidValue, v)
	}
	return nil
}

func (s TriggerSpec) fireReload(ctx context.Context, _ string) error {
	if s.OnTrigger == nil {
		return nil
	}
	return s.OnTrigger(ctx)
}

func (s TriggerSpec) kind() string { return "trigger" }

// Uint32Spec is a typed spec for uint32 config keys.
type Uint32Spec struct {
	Default  uint32
	OnReload func(context.Context, uint32) error
}

func (s Uint32Spec) defaultStr() string { return strconv.FormatUint(uint64(s.Default), 10) }

func (s Uint32Spec) validate(v string) error {
	if _, err := strconv.ParseUint(v, 10, 32); err != nil {
		return fmt.Errorf("%w: uint32 key requires a non-negative integer ≤ 4294967295, got %q", ErrInvalidValue, v)
	}
	return nil
}

func (s Uint32Spec) fireReload(ctx context.Context, v string) error {
	if s.OnReload == nil {
		return nil
	}
	n, _ := strconv.ParseUint(v, 10, 32)
	return s.OnReload(ctx, uint32(n))
}

func (s Uint32Spec) kind() string { return "uint32" }
