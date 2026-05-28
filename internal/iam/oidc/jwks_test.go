package oidc

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJWKSCacheStrictFailsClosedOnRefreshError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "down", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	cache := NewJWKSCache(http.DefaultClient, time.Minute)
	cfg := testIssuerConfig(t, srv.URL, FailurePolicyStrict, 0)

	_, err := cache.Key(context.Background(), cfg, "kid-1", time.Now())
	require.ErrorContains(t, err, "refresh jwks")
}

func TestJWKSCacheGraceUsesLastGoodUntilTTL(t *testing.T) {
	key := mustRSAKey(t)
	jwks := marshalJWKS(t, "kid-1", &key.PublicKey)
	ok := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !ok {
			http.Error(w, "down", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(jwks)
	}))
	t.Cleanup(srv.Close)

	cache := NewJWKSCache(http.DefaultClient, 10*time.Second)
	cfg := testIssuerConfig(t, srv.URL, FailurePolicyGrace, time.Minute)

	pub, err := cache.Key(context.Background(), cfg, "kid-1", time.Unix(100, 0))
	require.NoError(t, err)
	require.Equal(t, key.PublicKey.N, pub.N)

	ok = false
	pub, err = cache.Key(context.Background(), cfg, "kid-1", time.Unix(120, 0))
	require.NoError(t, err)
	require.Equal(t, key.PublicKey.N, pub.N)

	_, err = cache.Key(context.Background(), cfg, "kid-1", time.Unix(200, 0))
	require.ErrorContains(t, err, "grace")
}

func TestJWKSCacheRejectsMalformedJWKS(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"keys":[{"kty":"oct","kid":"kid-1"}]}`))
	}))
	t.Cleanup(srv.Close)

	cache := NewJWKSCache(http.DefaultClient, time.Minute)
	cfg := testIssuerConfig(t, srv.URL, FailurePolicyStrict, 0)

	_, err := cache.Key(context.Background(), cfg, "kid-1", time.Now())
	require.ErrorContains(t, err, "no RSA keys")
}

func TestJWKSCacheRejectsWeakRSAKey(t *testing.T) {
	weakN := new(big.Int).Lsh(big.NewInt(1), 511)
	doc := map[string]any{"keys": []map[string]string{{
		"kty": "RSA",
		"kid": "kid-1",
		"alg": "RS256",
		"use": "sig",
		"n":   base64.RawURLEncoding.EncodeToString(weakN.Bytes()),
		"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(65537).Bytes()),
	}}}
	body, err := json.Marshal(doc)
	require.NoError(t, err)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	cache := NewJWKSCache(http.DefaultClient, time.Minute)
	cfg := testIssuerConfig(t, srv.URL, FailurePolicyStrict, 0)

	_, err = cache.Key(context.Background(), cfg, "kid-1", time.Now())
	require.ErrorContains(t, err, "no RSA keys")
}

func TestJWKSCacheRejectsDuplicateKID(t *testing.T) {
	key1 := mustRSAKey(t)
	key2 := mustRSAKey(t)
	doc := map[string]any{"keys": []map[string]string{
		jwkDoc("kid-1", &key1.PublicKey),
		jwkDoc("kid-1", &key2.PublicKey),
	}}
	body, err := json.Marshal(doc)
	require.NoError(t, err)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	cache := NewJWKSCache(http.DefaultClient, time.Minute)
	cfg := testIssuerConfig(t, srv.URL, FailurePolicyStrict, 0)

	_, err = cache.Key(context.Background(), cfg, "kid-1", time.Now())
	require.ErrorContains(t, err, "duplicate kid")
}

func TestJWKSCacheRefreshesOnUnknownKIDInsideFreshTTL(t *testing.T) {
	key1 := mustRSAKey(t)
	key2 := mustRSAKey(t)
	jwks := marshalJWKS(t, "kid-1", &key1.PublicKey)
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(jwks)
	}))
	t.Cleanup(srv.Close)

	cache := NewJWKSCache(http.DefaultClient, time.Minute)
	cfg := testIssuerConfig(t, srv.URL, FailurePolicyStrict, 0)

	_, err := cache.Key(context.Background(), cfg, "kid-1", time.Unix(100, 0))
	require.NoError(t, err)
	require.Equal(t, 1, requests)

	jwks = marshalJWKS(t, "kid-2", &key2.PublicKey)
	pub, err := cache.Key(context.Background(), cfg, "kid-2", time.Unix(140, 0))
	require.NoError(t, err)
	require.Equal(t, key2.PublicKey.N, pub.N)
	require.Equal(t, 2, requests)
}

func TestJWKSCacheDoesNotOverwriteNewerRefreshWithOlderResponse(t *testing.T) {
	key1 := mustRSAKey(t)
	key2 := mustRSAKey(t)
	firstSeen := make(chan struct{})
	releaseFirst := make(chan struct{})
	var mu sync.Mutex
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		n := requests
		mu.Unlock()
		if n == 1 {
			close(firstSeen)
			<-releaseFirst
			_, _ = w.Write(marshalJWKS(t, "kid-1", &key1.PublicKey))
			return
		}
		_, _ = w.Write(marshalJWKS(t, "kid-2", &key2.PublicKey))
	}))
	t.Cleanup(srv.Close)

	cache := NewJWKSCache(http.DefaultClient, time.Millisecond)
	cfg := testIssuerConfig(t, srv.URL, FailurePolicyStrict, 0)
	errCh := make(chan error, 1)
	go func() {
		_, err := cache.Key(context.Background(), cfg, "kid-1", time.Unix(100, 0))
		errCh <- err
	}()
	<-firstSeen

	pub, err := cache.Key(context.Background(), cfg, "kid-2", time.Unix(200, 0))
	require.NoError(t, err)
	require.Equal(t, key2.PublicKey.N, pub.N)

	close(releaseFirst)
	require.ErrorContains(t, <-errCh, "kid")

	pub, err = cache.Key(context.Background(), cfg, "kid-2", time.Unix(201, 0))
	require.NoError(t, err)
	require.Equal(t, key2.PublicKey.N, pub.N)
}

func TestJWKSCacheGraceDoesNotUseOlderSnapshotAfterNewerRefresh(t *testing.T) {
	key1 := mustRSAKey(t)
	key2 := mustRSAKey(t)
	secondSeen := make(chan struct{})
	releaseSecond := make(chan struct{})
	var mu sync.Mutex
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		n := requests
		mu.Unlock()
		switch n {
		case 1:
			_, _ = w.Write(marshalJWKS(t, "kid-1", &key1.PublicKey))
		case 2:
			close(secondSeen)
			<-releaseSecond
			http.Error(w, "down", http.StatusInternalServerError)
		default:
			_, _ = w.Write(marshalJWKS(t, "kid-2", &key2.PublicKey))
		}
	}))
	t.Cleanup(srv.Close)

	cache := NewJWKSCache(http.DefaultClient, time.Millisecond)
	cfg := testIssuerConfig(t, srv.URL, FailurePolicyGrace, 5*time.Minute)
	_, err := cache.Key(context.Background(), cfg, "kid-1", time.Unix(100, 0))
	require.NoError(t, err)

	errCh := make(chan error, 1)
	go func() {
		_, err := cache.Key(context.Background(), cfg, "kid-1", time.Unix(200, 0))
		errCh <- err
	}()
	<-secondSeen

	pub, err := cache.Key(context.Background(), cfg, "kid-2", time.Unix(201, 0))
	require.NoError(t, err)
	require.Equal(t, key2.PublicKey.N, pub.N)

	close(releaseSecond)
	require.ErrorContains(t, <-errCh, "kid")
}

func TestJWKSCacheUnknownKIDRefreshIsRateLimited(t *testing.T) {
	key := mustRSAKey(t)
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(marshalJWKS(t, "kid-1", &key.PublicKey))
	}))
	t.Cleanup(srv.Close)

	cache := NewJWKSCache(http.DefaultClient, time.Minute)
	cfg := testIssuerConfig(t, srv.URL, FailurePolicyStrict, 0)

	_, err := cache.Key(context.Background(), cfg, "kid-1", time.Unix(100, 0))
	require.NoError(t, err)

	for _, kid := range []string{"kid-x", "kid-y", "kid-z"} {
		_, err = cache.Key(context.Background(), cfg, kid, time.Unix(110, 0))
		require.ErrorContains(t, err, "kid")
	}
	require.Equal(t, 2, requests)
}

func TestJWKSCacheRejectsRedirects(t *testing.T) {
	key := mustRSAKey(t)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(marshalJWKS(t, "kid-1", &key.PublicKey))
	}))
	t.Cleanup(target.Close)

	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	t.Cleanup(redirector.Close)

	cache := NewJWKSCache(http.DefaultClient, time.Minute)
	cfg := testIssuerConfig(t, redirector.URL, FailurePolicyStrict, 0)

	_, err := cache.Key(context.Background(), cfg, "kid-1", time.Now())
	require.ErrorContains(t, err, "redirect")
}

func testIssuerConfig(t *testing.T, jwksURL string, policy FailurePolicy, grace time.Duration) IssuerConfig {
	t.Helper()
	return IssuerConfig{
		Name:              "example",
		IssuerURL:         "https://idp.example.com/",
		Audience:          "grainfs",
		JWKSURL:           jwksURL,
		GroupsClaim:       "groups",
		GroupPrefix:       "oidc:example:",
		JWKSFailurePolicy: policy,
		GraceTTL:          grace,
	}
}

func mustRSAKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	return key
}

func marshalJWKS(t *testing.T, kid string, pub *rsa.PublicKey) []byte {
	t.Helper()
	doc := map[string]any{"keys": []map[string]string{jwkDoc(kid, pub)}}
	out, err := json.Marshal(doc)
	require.NoError(t, err)
	return out
}

func jwkDoc(kid string, pub *rsa.PublicKey) map[string]string {
	return map[string]string{
		"kty": "RSA",
		"kid": kid,
		"alg": "RS256",
		"use": "sig",
		"n":   base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
		"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes()),
	}
}
