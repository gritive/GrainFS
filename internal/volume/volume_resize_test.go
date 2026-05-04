package volume

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestManager_Resize_Grow(t *testing.T) {
	m := setupManager(t)
	if _, err := m.Create("v1", 1<<20); err != nil {
		t.Fatal(err)
	}
	if err := m.Resize("v1", 1<<21); err != nil {
		t.Fatalf("grow err = %v", err)
	}
	v, err := m.Get("v1")
	require.NoError(t, err)
	if v.Size != 1<<21 {
		t.Fatalf("size after grow = %d, want %d", v.Size, 1<<21)
	}
}

func TestManager_Resize_ShrinkRejected(t *testing.T) {
	m := setupManager(t)
	if _, err := m.Create("v1", 1<<21); err != nil {
		t.Fatal(err)
	}
	err := m.Resize("v1", 1<<20)
	if !errors.Is(err, ErrShrinkNotSupported) {
		t.Fatalf("err = %v, want ErrShrinkNotSupported", err)
	}
	v, _ := m.Get("v1")
	if v.Size != 1<<21 {
		t.Fatalf("size after rejected shrink = %d, want unchanged %d", v.Size, 1<<21)
	}
}

func TestManager_Resize_EqualNoOp(t *testing.T) {
	m := setupManager(t)
	if _, err := m.Create("v1", 1<<20); err != nil {
		t.Fatal(err)
	}
	if err := m.Resize("v1", 1<<20); err != nil {
		t.Fatalf("equal resize err = %v", err)
	}
	v, _ := m.Get("v1")
	if v.Size != 1<<20 {
		t.Fatalf("size = %d, want %d", v.Size, 1<<20)
	}
}

func TestManager_Resize_NotFound(t *testing.T) {
	m := setupManager(t)
	err := m.Resize("ghost", 1<<20)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestManager_Resize_InvalidSize(t *testing.T) {
	m := setupManager(t)
	if _, err := m.Create("v1", 1<<20); err != nil {
		t.Fatal(err)
	}
	err := m.Resize("v1", 0)
	if !errors.Is(err, ErrInvalidSize) {
		t.Fatalf("err = %v, want ErrInvalidSize", err)
	}
	err = m.Resize("v1", -100)
	if !errors.Is(err, ErrInvalidSize) {
		t.Fatalf("err on negative = %v, want ErrInvalidSize", err)
	}
}

func TestManager_Resize_ConcurrentReadIsSafe(t *testing.T) {
	m := setupManager(t)
	if _, err := m.Create("v1", 1<<20); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); _, _ = m.Get("v1") }()
		go func() { defer wg.Done(); _ = m.Resize("v1", 1<<22) }()
	}
	wg.Wait()
	v, _ := m.Get("v1")
	if v.Size < 1<<20 {
		t.Fatalf("size shrank under concurrency: %d", v.Size)
	}
}
