// Package dashboard provides the auth token store for the GrainFS web dashboard.
//
// The dashboard uses a single shared token persisted at <data>/dashboard.token
// with mode 0600. CLI command `grainfs dashboard` returns a URL with the token
// embedded as a fragment so the browser can store it in localStorage and send
// it as Authorization: Bearer <token> on every fetch.
//
// `--rotate` overwrites the file; existing browser sessions hit 401 and the
// operator runs the CLI again to get a new URL.
package dashboard

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
)

const tokenBytes = 32

// TokenStore loads, persists, and rotates the dashboard auth token.
// Concurrent-safe; callers may share a single TokenStore across goroutines.
type TokenStore struct {
	path string
	mu   sync.RWMutex
	val  string
}

// Open loads the token from path or creates a new random token there with mode 0600.
// If the file exists but is empty, a fresh token is generated and persisted.
func Open(path string) (*TokenStore, error) {
	s := &TokenStore{path: path}
	if data, err := os.ReadFile(path); err == nil {
		s.val = string(data)
		if len(s.val) == 0 {
			if _, err := s.rotateLocked(); err != nil {
				return nil, err
			}
		}
		return s, nil
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read token: %w", err)
	}
	if _, err := s.rotateLocked(); err != nil {
		return nil, err
	}
	return s, nil
}

// Get returns the current token.
func (s *TokenStore) Get() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.val
}

// Rotate generates a new token, persists it (mode 0600), and returns it.
// Existing tokens stop verifying immediately.
func (s *TokenStore) Rotate() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rotateLocked()
}

func (s *TokenStore) rotateLocked() (string, error) {
	buf := make([]byte, tokenBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("rand: %w", err)
	}
	tok := hex.EncodeToString(buf)
	if err := os.WriteFile(s.path, []byte(tok), 0o600); err != nil {
		return "", fmt.Errorf("write token: %w", err)
	}
	s.val = tok
	return tok, nil
}

// Verify reports whether presented matches the current token using constant-time
// comparison. An empty presented value always returns false.
func (s *TokenStore) Verify(presented string) bool {
	if presented == "" {
		return false
	}
	cur := s.Get()
	if len(cur) != len(presented) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(cur), []byte(presented)) == 1
}

// Path returns the file path the token is persisted at.
func (s *TokenStore) Path() string { return s.path }
