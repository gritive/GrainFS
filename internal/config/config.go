// Package config provides a cluster-wide config registry backed by the meta-FSM.
// Each key has a typed Spec (BoolSpec, StringSpec, TriggerSpec, Uint32Spec) with an
// optional reload-hook callback that fires on every successful Set.
package config

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrUnknownKey is returned when Set/Unset/Get is called for an unregistered key.
var ErrUnknownKey = errors.New("unknown config key")

// ErrInvalidValue is returned when the value does not satisfy the key's Spec.
var ErrInvalidValue = errors.New("invalid value for config key")

// Entry is a snapshot of one registered key as returned by ListAll.
type Entry struct {
	Key     string
	Value   string
	Kind    string
	Default string
	Set     bool // true if an explicit value overrides the default
}

// Store is the in-memory config registry. It is safe for concurrent reads.
//
// Write path (Set / Unset / Restore) must be called from a single serialized
// goroutine — typically the Raft FSM apply loop. Under those conditions the
// "read prev → write new → fire hook → restore prev on failure" rollback in Set
// is safe because no concurrent Set can race for the same key.
// DO NOT call Set concurrently from multiple goroutines outside the FSM apply path.
type Store struct {
	mu     sync.RWMutex
	specs  map[string]Spec
	values map[string]string // only keys with explicit (non-default) values
}

// NewStore returns an empty Store with no registered keys.
func NewStore() *Store {
	return &Store{
		specs:  make(map[string]Spec),
		values: make(map[string]string),
	}
}

// Register adds a key with its typed Spec. Must be called before the store is used.
func (s *Store) Register(key string, spec Spec) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.specs[key] = spec
}

// Set validates and applies a new value for key, then fires the spec's reload hook.
// If the hook returns an error the value is rolled back and the error is returned.
//
// Concurrency note: see Store doc comment — call only from the FSM apply path.
func (s *Store) Set(ctx context.Context, key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	spec, ok := s.specs[key]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownKey, key)
	}

	if err := spec.validate(value); err != nil {
		return err
	}

	// Save previous state for rollback.
	prev, hadPrev := s.values[key]
	s.values[key] = value

	if err := spec.fireReload(ctx, value); err != nil {
		// Rollback.
		if hadPrev {
			s.values[key] = prev
		} else {
			delete(s.values, key)
		}
		return err
	}
	return nil
}

// Unset removes an explicit override, reverting the key to its default value.
func (s *Store) Unset(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	spec, ok := s.specs[key]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownKey, key)
	}

	prev, hadPrev := s.values[key]
	delete(s.values, key)

	if err := spec.fireReload(ctx, spec.defaultStr()); err != nil {
		// Rollback.
		if hadPrev {
			s.values[key] = prev
		}
		return err
	}
	return nil
}

// GetString returns the current string value of key and whether it is registered.
func (s *Store) GetString(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	spec, ok := s.specs[key]
	if !ok {
		return "", false
	}
	if v, set := s.values[key]; set {
		return v, true
	}
	return spec.defaultStr(), true
}

// GetBool returns the current boolean value of key and whether it is registered.
// Returns (false, false) if the key is not registered or not a bool key.
func (s *Store) GetBool(key string) (bool, bool) {
	v, ok := s.GetString(key)
	if !ok {
		return false, false
	}
	return v == "true", true
}

// ListAll returns a snapshot of all registered keys with their current values.
func (s *Store) ListAll() []Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := make([]Entry, 0, len(s.specs))
	for key, spec := range s.specs {
		v, set := s.values[key]
		if !set {
			v = spec.defaultStr()
		}
		entries = append(entries, Entry{
			Key:     key,
			Value:   v,
			Kind:    spec.kind(),
			Default: spec.defaultStr(),
			Set:     set,
		})
	}
	return entries
}

// Snapshot returns a copy of all explicitly-set key→value pairs for persistence.
func (s *Store) Snapshot() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make(map[string]string, len(s.values))
	for k, v := range s.values {
		out[k] = v
	}
	return out
}

// Restore replaces all explicit values with those from a persisted snapshot.
// Keys not present in values revert to their default. Unknown keys are silently ignored.
func (s *Store) Restore(values map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Clear current overrides.
	for k := range s.values {
		delete(s.values, k)
	}
	// Apply snapshot values only for registered keys.
	for k, v := range values {
		if _, ok := s.specs[k]; ok {
			s.values[k] = v
		}
	}
}
