package iam

import (
	"errors"
	"slices"
	"testing"
)

func TestNormalizeScope(t *testing.T) {
	tests := []struct {
		name    string
		in      []string
		want    []string
		wantErr bool
	}{
		{"nil", nil, nil, false},
		{"empty", []string{}, nil, false},
		{"happy_sort", []string{"b", "a"}, []string{"a", "b"}, false},
		{"dedup", []string{"a", "b", "a"}, []string{"a", "b"}, false},
		{"empty_string", []string{"a", ""}, nil, true},
		{"whitespace_only", []string{"a", "   "}, nil, true},
		{"wildcard_sentinel", []string{"a", "*"}, nil, true},
		{"system_sentinel", []string{"__system__"}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NormalizeScope(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && !slices.Equal(got, tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestScopeAllows(t *testing.T) {
	tests := []struct {
		name   string
		scope  []string
		bucket string
		want   bool
	}{
		{"nil scope unrestricted", nil, "any", true},
		{"empty scope unrestricted", []string{}, "any", true},
		{"in scope", []string{"a", "b"}, "a", true},
		{"out of scope", []string{"a", "b"}, "c", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ScopeAllows(tt.scope, tt.bucket); got != tt.want {
				t.Fatalf("ScopeAllows(%v, %q) = %v, want %v", tt.scope, tt.bucket, got, tt.want)
			}
		})
	}
}

func TestNormalizeScope_TypedErrors(t *testing.T) {
	if _, err := NormalizeScope([]string{"*"}); !errors.Is(err, ErrScopeSentinel) {
		t.Fatalf("err for wildcard = %v, want ErrScopeSentinel", err)
	}
	if _, err := NormalizeScope([]string{"__system__"}); !errors.Is(err, ErrScopeSentinel) {
		t.Fatalf("err for system = %v, want ErrScopeSentinel", err)
	}
	if _, err := NormalizeScope([]string{""}); !errors.Is(err, ErrScopeEmptyEntry) {
		t.Fatalf("err for empty = %v, want ErrScopeEmptyEntry", err)
	}
	if _, err := NormalizeScope([]string{" "}); !errors.Is(err, ErrScopeEmptyEntry) {
		t.Fatalf("err for whitespace = %v, want ErrScopeEmptyEntry", err)
	}
}
