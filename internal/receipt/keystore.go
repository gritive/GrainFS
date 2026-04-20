package receipt

import (
	"errors"
	"fmt"
	"sync"
)

// Key is a single HMAC signing key identified by ID. Secret must not be
// shared with untrusted code: the KeyStore never copies it out.
type Key struct {
	ID     string
	Secret []byte
}

// defaultPreviousRetention keeps the three most recently rotated-out keys so
// audits during a rotation window still verify. Operators can override via
// WithPreviousRetention.
const defaultPreviousRetention = 3

// KeyStore holds the active signing key plus a bounded window of previous
// keys used for verification only. It is safe for concurrent use.
type KeyStore struct {
	mu                sync.RWMutex
	active            Key
	previous          []Key // oldest first
	previousRetention int
}

// Option customizes NewKeyStore.
type Option func(*KeyStore)

// WithPreviousRetention sets how many rotated-out keys to retain for
// verification. The default is 3.
func WithPreviousRetention(n int) Option {
	return func(ks *KeyStore) {
		if n < 0 {
			n = 0
		}
		ks.previousRetention = n
	}
}

// NewKeyStore returns a store with the given initial active key.
func NewKeyStore(active Key, opts ...Option) (*KeyStore, error) {
	if err := validateKey(active); err != nil {
		return nil, err
	}
	ks := &KeyStore{
		active:            active,
		previousRetention: defaultPreviousRetention,
	}
	for _, opt := range opts {
		opt(ks)
	}
	return ks, nil
}

// Active returns the current signing key. The bool is false only when the
// store was zero-initialized.
func (ks *KeyStore) Active() (Key, bool) {
	if ks == nil {
		return Key{}, false
	}
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	if ks.active.ID == "" {
		return Key{}, false
	}
	return ks.active, true
}

// Lookup returns the key for id, searching active first then the retention
// window. The bool is false when id is unknown or expired.
func (ks *KeyStore) Lookup(id string) (Key, bool) {
	if ks == nil || id == "" {
		return Key{}, false
	}
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	if ks.active.ID == id {
		return ks.active, true
	}
	for _, p := range ks.previous {
		if p.ID == id {
			return p, true
		}
	}
	return Key{}, false
}

// Rotate promotes next to active, demotes the old active into the previous
// window, and evicts any keys beyond previousRetention. Reusing the current
// active ID is rejected to avoid silent key reuse.
func (ks *KeyStore) Rotate(next Key) error {
	if err := validateKey(next); err != nil {
		return err
	}
	ks.mu.Lock()
	defer ks.mu.Unlock()

	if ks.active.ID == next.ID {
		return fmt.Errorf("receipt: cannot rotate to the already-active key id %q", next.ID)
	}
	for _, p := range ks.previous {
		if p.ID == next.ID {
			return fmt.Errorf("receipt: cannot rotate to a previous key id %q", next.ID)
		}
	}

	ks.previous = append(ks.previous, ks.active)
	if overflow := len(ks.previous) - ks.previousRetention; overflow > 0 {
		ks.previous = ks.previous[overflow:]
	}
	ks.active = next
	return nil
}

func validateKey(k Key) error {
	if k.ID == "" {
		return errors.New("receipt: key ID must not be empty")
	}
	if len(k.Secret) == 0 {
		return errors.New("receipt: key secret must not be empty")
	}
	return nil
}
