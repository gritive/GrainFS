package oidc

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"sync"
	"time"
)

const (
	jwksMaxBytes              = 1 << 20
	unknownKIDRefreshCooldown = 30 * time.Second
)

type JWKSCache struct {
	client *http.Client
	ttl    time.Duration
	mu     sync.Mutex
	sets   map[string]cachedJWKS
}

type cachedJWKS struct {
	keys                  map[string]*rsa.PublicKey
	loadedAt              time.Time
	lastUnknownKIDRefresh time.Time
}

func NewJWKSCache(client *http.Client, ttl time.Duration) *JWKSCache {
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	copyClient := *client
	if copyClient.Timeout <= 0 {
		copyClient.Timeout = 5 * time.Second
	}
	copyClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return errors.New("jwks redirects disabled")
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &JWKSCache{client: &copyClient, ttl: ttl, sets: make(map[string]cachedJWKS)}
}

func (c *JWKSCache) Key(ctx context.Context, cfg IssuerConfig, kid string, now time.Time) (*rsa.PublicKey, error) {
	if kid == "" {
		return nil, errors.New("kid required")
	}
	cacheKey := jwksCacheKey(cfg)
	c.mu.Lock()
	cached, ok := c.sets[cacheKey]
	unknownKIDRefresh := false
	if ok && now.Sub(cached.loadedAt) < c.ttl {
		key, found := cached.keys[kid]
		if found {
			c.mu.Unlock()
			return key, nil
		}
		if !cached.lastUnknownKIDRefresh.IsZero() && now.Sub(cached.lastUnknownKIDRefresh) < unknownKIDRefreshCooldown {
			c.mu.Unlock()
			return nil, fmt.Errorf("kid %q not found in jwks", kid)
		}
		cached.lastUnknownKIDRefresh = now
		c.sets[cacheKey] = cached
		unknownKIDRefresh = true
		c.mu.Unlock()
	} else {
		c.mu.Unlock()
	}

	refreshed, err := c.refresh(ctx, cfg, now)
	if err == nil {
		if unknownKIDRefresh {
			refreshed.lastUnknownKIDRefresh = now
		}
		c.mu.Lock()
		current, hasCurrent := c.sets[cacheKey]
		if hasCurrent && current.loadedAt.After(refreshed.loadedAt) {
			c.mu.Unlock()
			key, found := current.keys[kid]
			if !found {
				return nil, fmt.Errorf("kid %q not found in newer jwks", kid)
			}
			return key, nil
		}
		c.sets[cacheKey] = refreshed
		c.mu.Unlock()
		key, found := refreshed.keys[kid]
		if !found {
			return nil, fmt.Errorf("kid %q not found in jwks", kid)
		}
		return key, nil
	}
	if cfg.JWKSFailurePolicy == FailurePolicyGrace && ok {
		fallback := cached
		c.mu.Lock()
		current, hasCurrent := c.sets[cacheKey]
		if hasCurrent && current.loadedAt.After(fallback.loadedAt) {
			fallback = current
		}
		c.mu.Unlock()
		if now.Sub(fallback.loadedAt) > cfg.GraceTTL {
			return nil, fmt.Errorf("refresh jwks: %w; grace expired", err)
		}
		key, found := fallback.keys[kid]
		if !found {
			return nil, fmt.Errorf("kid %q not found in cached jwks", kid)
		}
		return key, nil
	}
	return nil, fmt.Errorf("refresh jwks: %w", err)
}

func jwksCacheKey(cfg IssuerConfig) string {
	return cfg.Name + "\x00" + cfg.IssuerURL + "\x00" + cfg.JWKSURL
}

func (c *JWKSCache) refresh(ctx context.Context, cfg IssuerConfig, now time.Time) (cachedJWKS, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.JWKSURL, nil)
	if err != nil {
		return cachedJWKS{}, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		return cachedJWKS{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return cachedJWKS{}, fmt.Errorf("status %d", resp.StatusCode)
	}
	var doc struct {
		Keys []jwkRSA `json:"keys"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, jwksMaxBytes)).Decode(&doc); err != nil {
		return cachedJWKS{}, err
	}
	keys := make(map[string]*rsa.PublicKey)
	for _, jwk := range doc.Keys {
		pub, err := jwk.publicKey()
		if err != nil {
			continue
		}
		if _, exists := keys[jwk.Kid]; exists {
			return cachedJWKS{}, fmt.Errorf("duplicate kid %q in jwks", jwk.Kid)
		}
		keys[jwk.Kid] = pub
	}
	if len(keys) == 0 {
		return cachedJWKS{}, errors.New("no RSA keys in jwks")
	}
	return cachedJWKS{keys: keys, loadedAt: now}, nil
}

type jwkRSA struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Alg string `json:"alg"`
	Use string `json:"use"`
	N   string `json:"n"`
	E   string `json:"e"`
}

func (j jwkRSA) publicKey() (*rsa.PublicKey, error) {
	if j.Kty != "RSA" || j.Kid == "" || j.N == "" || j.E == "" {
		return nil, errors.New("not an RSA signing key")
	}
	if j.Alg != "" && j.Alg != "RS256" {
		return nil, errors.New("unsupported alg")
	}
	if j.Use != "" && j.Use != "sig" {
		return nil, errors.New("unsupported use")
	}
	nBytes, err := base64.RawURLEncoding.DecodeString(j.N)
	if err != nil {
		return nil, err
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(j.E)
	if err != nil {
		return nil, err
	}
	n := new(big.Int).SetBytes(nBytes)
	if n.BitLen() < 2048 {
		return nil, errors.New("rsa modulus must be at least 2048 bits")
	}
	e := new(big.Int).SetBytes(eBytes).Int64()
	if e != 65537 {
		return nil, errors.New("invalid exponent")
	}
	return &rsa.PublicKey{N: n, E: int(e)}, nil
}
