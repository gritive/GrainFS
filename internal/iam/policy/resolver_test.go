package policy

import (
	"context"
	"testing"
	"time"
)

type fakeStore struct {
	saToPols     map[string][]string
	saToGroups   map[string][]string
	groupToPols  map[string][]string
	bucketPols   map[string]string
	docs         map[string]string
	resolveCount int
}

func (f *fakeStore) SAPolicies(_ context.Context, saID string) ([]string, error) {
	f.resolveCount++
	return f.saToPols[saID], nil
}

func (f *fakeStore) SAGroups(_ context.Context, saID string) ([]string, error) {
	return f.saToGroups[saID], nil
}

func (f *fakeStore) GroupPolicies(_ context.Context, group string) ([]string, error) {
	return f.groupToPols[group], nil
}

func (f *fakeStore) PolicyDoc(_ context.Context, name string) (*Document, error) {
	raw, ok := f.docs[name]
	if !ok {
		return nil, nil
	}
	return Parse([]byte(raw))
}

func (f *fakeStore) BucketPolicy(_ context.Context, bucket string) (*Document, error) {
	raw, ok := f.bucketPols[bucket]
	if !ok {
		return nil, nil
	}
	return Parse([]byte(raw))
}

func TestResolver_CachesUntilTTL(t *testing.T) {
	s := &fakeStore{
		saToPols: map[string][]string{"sa-1": {"readonly"}},
		docs:     map[string]string{"readonly": `{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`},
	}
	r := NewResolver(s, 100*time.Millisecond)
	if _, err := r.Effective(context.Background(), "sa-1", "bucket-x"); err != nil {
		t.Fatalf("Effective#1: %v", err)
	}
	if _, err := r.Effective(context.Background(), "sa-1", "bucket-x"); err != nil {
		t.Fatalf("Effective#2: %v", err)
	}
	if s.resolveCount > 1 {
		t.Fatalf("cache miss on second call: resolveCount=%d", s.resolveCount)
	}
	time.Sleep(150 * time.Millisecond)
	if _, err := r.Effective(context.Background(), "sa-1", "bucket-x"); err != nil {
		t.Fatalf("Effective#3: %v", err)
	}
	if s.resolveCount != 2 {
		t.Fatalf("TTL did not expire: resolveCount=%d", s.resolveCount)
	}
}

func TestResolver_InvalidateClearsImmediately(t *testing.T) {
	s := &fakeStore{
		saToPols: map[string][]string{"sa-1": {"readonly"}},
		docs:     map[string]string{"readonly": `{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`},
	}
	r := NewResolver(s, 1*time.Hour)
	if _, err := r.Effective(context.Background(), "sa-1", "bucket-x"); err != nil {
		t.Fatal(err)
	}
	before := s.resolveCount
	r.Invalidate([]string{"sa-1"}, nil)
	if _, err := r.Effective(context.Background(), "sa-1", "bucket-x"); err != nil {
		t.Fatal(err)
	}
	if s.resolveCount != before+1 {
		t.Fatalf("invalidate did not force re-resolve: before=%d after=%d", before, s.resolveCount)
	}
}
