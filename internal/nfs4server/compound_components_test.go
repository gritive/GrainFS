package nfs4server

import (
	"testing"
)

// newTestDispatcher creates a Dispatcher with a state manager but no backend.
// Operations that need the backend (e.g., opCreate non-trivial paths) are out
// of scope for these tests — we only verify component-name rejection at op
// entry, which fires before backend access.
func newTestDispatcher(_ *testing.T) *Dispatcher {
	return &Dispatcher{
		state: NewStateManager(),
	}
}

// xdrString builds an NFSv4 component4 (XDR string<>) payload: 4-byte length
// prefix + bytes + 0..3 zero-byte alignment padding.
func xdrString(s string) []byte {
	out := make([]byte, 0, 4+len(s)+3)
	n := len(s)
	out = append(out,
		byte(n>>24), byte(n>>16), byte(n>>8), byte(n),
	)
	out = append(out, []byte(s)...)
	for len(out)%4 != 0 {
		out = append(out, 0)
	}
	return out
}

// xdrUint32 emits a single 4-byte big-endian uint32.
func xdrUint32(v uint32) []byte {
	return []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}

func TestOpLookup_RejectsInvalidComponent(t *testing.T) {
	d := newTestDispatcher(t)
	for _, name := range []string{"", ".", ".."} {
		t.Run("name="+name, func(t *testing.T) {
			res := d.opLookup([]byte(name))
			if res.Status != NFS4ERR_INVAL {
				t.Fatalf("name=%q: want NFS4ERR_INVAL, got %d", name, res.Status)
			}
		})
	}
}

func TestOpCreate_RejectsInvalidComponent(t *testing.T) {
	d := newTestDispatcher(t)
	for _, name := range []string{"", ".", ".."} {
		t.Run("name="+name, func(t *testing.T) {
			payload := append(xdrUint32(NF4DIR), xdrString(name)...)
			res := d.opCreate(payload)
			if res.Status != NFS4ERR_INVAL {
				t.Fatalf("name=%q: want NFS4ERR_INVAL, got %d", name, res.Status)
			}
		})
	}
}

func TestOpOpen_RejectsInvalidComponent(t *testing.T) {
	d := newTestDispatcher(t)
	for _, name := range []string{"", ".", ".."} {
		t.Run("name="+name, func(t *testing.T) {
			// shareAccess(4) + openType(4) + name(string)
			payload := append(xdrUint32(0), xdrUint32(0)...)
			payload = append(payload, xdrString(name)...)
			res := d.opOpen(payload)
			if res.Status != NFS4ERR_INVAL {
				t.Fatalf("name=%q: want NFS4ERR_INVAL, got %d", name, res.Status)
			}
		})
	}
}

func TestOpRemove_RejectsInvalidComponent(t *testing.T) {
	d := newTestDispatcher(t)
	for _, name := range []string{"", ".", ".."} {
		t.Run("name="+name, func(t *testing.T) {
			res := d.opRemove([]byte(name))
			if res.Status != NFS4ERR_INVAL {
				t.Fatalf("name=%q: want NFS4ERR_INVAL, got %d", name, res.Status)
			}
		})
	}
}

func TestOpRename_RejectsInvalidComponent(t *testing.T) {
	d := newTestDispatcher(t)
	cases := []struct {
		label   string
		oldName string
		newName string
	}{
		{"old=..", "..", "valid"},
		{"new=..", "valid", ".."},
		{"old=.", ".", "valid"},
		{"new=.", "valid", "."},
		{"old=empty", "", "valid"},
		{"new=empty", "valid", ""},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			payload := append(xdrString(tc.oldName), xdrString(tc.newName)...)
			res := d.opRename(payload)
			if res.Status != NFS4ERR_INVAL {
				t.Fatalf("oldname=%q newname=%q: want NFS4ERR_INVAL, got %d",
					tc.oldName, tc.newName, res.Status)
			}
		})
	}
}
