package cluster

import (
	"bytes"
	"strings"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
)

func TestStateKeyspace_EmptyPrefixIsIdentity(t *testing.T) {
	ks := newStateKeyspaceEmpty()
	raw := []byte("obj:bucket/key")
	if got := ks.Key(raw); !bytes.Equal(got, raw) {
		t.Fatalf("Key with empty prefix = %q, want %q", got, raw)
	}
	if got := ks.Prefix([]byte("obj:")); !bytes.Equal(got, []byte("obj:")) {
		t.Fatalf("Prefix with empty prefix = %q, want %q", got, "obj:")
	}
	if got := ks.MustStrip(raw); !bytes.Equal(got, raw) {
		t.Fatalf("MustStrip with empty prefix = %q, want %q", got, raw)
	}
	if got, ok := ks.TryStrip(raw); !ok || !bytes.Equal(got, raw) {
		t.Fatalf("TryStrip with empty prefix = (%q,%v), want (%q,true)", got, ok, raw)
	}
	if !ks.HasPrefix(raw) {
		t.Fatal("HasPrefix on empty keyspace should always be true")
	}
}

func TestStateKeyspace_PrefixRoundTrip(t *testing.T) {
	ks, err := newStateKeyspace("group-3")
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte("obj:bucket/key")
	full := ks.Key(raw)
	if bytes.Equal(full, raw) {
		t.Fatal("expected prefixed key to differ from raw")
	}
	if !ks.HasPrefix(full) {
		t.Fatal("HasPrefix(full) = false")
	}
	if ks.HasPrefix(raw) {
		t.Fatal("HasPrefix(raw) = true, want false")
	}
	if got := ks.MustStrip(full); !bytes.Equal(got, raw) {
		t.Fatalf("MustStrip round-trip = %q, want %q", got, raw)
	}
	if got, ok := ks.TryStrip(full); !ok || !bytes.Equal(got, raw) {
		t.Fatalf("TryStrip round-trip = (%q,%v), want (%q,true)", got, ok, raw)
	}
	if _, ok := ks.TryStrip([]byte("nope")); ok {
		t.Fatal("TryStrip on un-prefixed key returned ok=true")
	}
}

func TestStateKeyspace_MustStripPanicsOnLeak(t *testing.T) {
	ks, _ := newStateKeyspace("group-3")
	defer func() {
		if recover() == nil {
			t.Fatal("MustStrip on un-prefixed key did not panic")
		}
	}()
	ks.MustStrip([]byte("obj:foreign"))
}

func TestStateKeyspace_NoCrossGroupCollision_PathologicalIDs(t *testing.T) {
	ids := []string{
		"g", "g:1", "group", "groupX", // prefix-of-prefix candidates
		strings.Repeat("a", 300),    // length >= 256
		"with\x00nul", "with:colon", // weird bytes
	}
	keyspaces := map[string]*stateKeyspace{}
	for _, id := range ids {
		ks, err := newStateKeyspace(id)
		if err != nil {
			t.Fatalf("newStateKeyspace(%q): %v", id, err)
		}
		keyspaces[id] = ks
	}
	if _, err := newStateKeyspace(""); err == nil {
		t.Fatal("newStateKeyspace(\"\") should error")
	}
	raw := []byte("obj:bucket/key")
	seen := map[string]string{}
	for id, ks := range keyspaces {
		enc := string(ks.Key(raw))
		if other, dup := seen[enc]; dup {
			t.Fatalf("encoded key collision: %q and %q both produce %q", id, other, enc)
		}
		seen[enc] = id
		for otherID, otherKS := range keyspaces {
			if otherID == id {
				continue
			}
			if otherKS.HasPrefix(ks.Key(raw)) {
				t.Fatalf("group %q's key matches group %q's prefix", id, otherID)
			}
		}
	}
}

func TestStateKeyspace_ScanGroupPrefix(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	ksA, _ := newStateKeyspace("group-A")
	ksB, _ := newStateKeyspace("group-B")
	// Write obj: keys for two groups + a non-obj key for group A.
	if err := db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(ksA.Key([]byte("obj:b/k1")), []byte("A1")); err != nil {
			return err
		}
		if err := txn.Set(ksA.Key([]byte("obj:b/k2")), []byte("A2")); err != nil {
			return err
		}
		if err := txn.Set(ksA.Key([]byte("bucket:b")), []byte("Abucket")); err != nil {
			return err
		}
		if err := txn.Set(ksB.Key([]byte("obj:b/k1")), []byte("B1")); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Scoped scan: group A's obj: prefix returns exactly k1,k2 with stripped keys.
	got := map[string]string{}
	if err := db.View(func(txn *badger.Txn) error {
		return ksA.scanGroupPrefix(txn, []byte("obj:"), func(raw []byte, item *badger.Item) error {
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			got[string(raw)] = string(v)
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"obj:b/k1": "A1", "obj:b/k2": "A2"}
	if len(got) != 2 || got["obj:b/k1"] != "A1" || got["obj:b/k2"] != "A2" {
		t.Fatalf("scanGroupPrefix(A, obj:) = %v, want %v", got, want)
	}

	// errStopScan halts cleanly (returns nil), having seen >= 1 item.
	n := 0
	if err := db.View(func(txn *badger.Txn) error {
		return ksA.scanGroupPrefix(txn, []byte("obj:"), func(raw []byte, item *badger.Item) error {
			n++
			return errStopScan
		})
	}); err != nil {
		t.Fatalf("scanGroupPrefix with errStopScan returned %v, want nil", err)
	}
	if n != 1 {
		t.Fatalf("errStopScan scan visited %d items, want 1", n)
	}

	// Empty keyspace: scans the raw prefix across the whole DB (here that's all
	// obj: keys regardless of group prefix — note in shared mode the empty
	// keyspace is only used for single-group tests, so this is the expected
	// "no group prefix => whole DB" behavior).
	ksEmpty := newStateKeyspaceEmpty()
	cnt := 0
	if err := db.View(func(txn *badger.Txn) error {
		return ksEmpty.scanGroupPrefix(txn, []byte("obj:"), func(raw []byte, item *badger.Item) error {
			cnt++
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	// The DB has 3 obj: keys total (A1,A2 under group-A's prefix, B1 under group-B's),
	// but with an EMPTY keyspace, opts.Prefix = []byte("obj:") only matches keys that
	// LITERALLY start with "obj:" — and the group-prefixed ones start with the 4-byte
	// length, not "obj:". So cnt should be 0 here. (If your scanGroupPrefix for the empty
	// keyspace does something different, adjust this assertion to match the actual
	// documented behavior and explain it in your report.)
	if cnt != 0 {
		t.Fatalf("empty-keyspace scanGroupPrefix(obj:) visited %d keys; expected 0 (group-prefixed keys don't literally start with \"obj:\")", cnt)
	}
}
