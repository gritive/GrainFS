package nfs4server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_Remove verifies that REMOVE deletes the target file.
func TestE2E_Remove(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	c := newNFS4Client(t, addr)

	// Create a file
	c.writeTestFile("to-delete.txt", []byte("goodbye"))

	// PUTROOTFH + REMOVE "to-delete.txt"
	ops := &XDRWriter{}
	ops.WriteUint32(uint32(OpPutRootFH))
	ops.WriteUint32(uint32(OpRemove))
	ops.WriteString("to-delete.txt")

	reply := c.sendCompound(ops.Bytes(), 2)
	status, _ := c.parseCompoundReply(reply)
	require.Equal(t, uint32(NFS4_OK), status, "REMOVE should succeed")

	// Verify file is gone via READDIR
	ops2 := &XDRWriter{}
	ops2.WriteUint32(uint32(OpPutRootFH))
	ops2.WriteUint32(uint32(OpReadDir))
	ops2.WriteUint64(0)    // cookie
	ops2.WriteUint64(0)    // cookieverf
	ops2.WriteUint32(4096) // dircount
	ops2.WriteUint32(4096) // maxcount
	ops2.WriteUint32(0)    // attr bitmap len = 0

	reply2 := c.sendCompound(ops2.Bytes(), 2)
	status2, r := c.parseCompoundReply(reply2)
	require.Equal(t, uint32(NFS4_OK), status2)

	// Skip PUTROOTFH result
	r.ReadUint32()
	r.ReadUint32()

	// Parse READDIR result
	r.ReadUint32() // op code
	r.ReadUint32() // status
	r.ReadUint64() // cookieverf

	var names []string
	for {
		follows, err := r.ReadUint32()
		if err != nil || follows == 0 {
			break
		}
		r.ReadUint64() // cookie
		name, _ := r.ReadString()
		r.ReadUint32() // bitmap len
		r.ReadOpaque() // attrvals
		names = append(names, name)
	}

	assert.NotContains(t, names, "to-delete.txt", "deleted file should not appear in READDIR")
}

// TestE2E_Remove_NonExistent verifies that REMOVE on a missing file returns NFS4ERR_NOENT.
func TestE2E_Remove_NonExistent(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	c := newNFS4Client(t, addr)

	ops := &XDRWriter{}
	ops.WriteUint32(uint32(OpPutRootFH))
	ops.WriteUint32(uint32(OpRemove))
	ops.WriteString("ghost.txt")

	reply := c.sendCompound(ops.Bytes(), 2)
	status, _ := c.parseCompoundReply(reply)
	assert.Equal(t, uint32(NFS4ERR_NOENT), status, "REMOVE of non-existent file should return NOENT")
}

// TestE2E_Rename verifies that RENAME moves a file to the new name.
func TestE2E_Rename(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	c := newNFS4Client(t, addr)

	c.writeTestFile("old-name.txt", []byte("contents"))

	// PUTROOTFH + RENAME "old-name.txt" -> "new-name.txt"
	ops := &XDRWriter{}
	ops.WriteUint32(uint32(OpPutRootFH))
	ops.WriteUint32(uint32(OpRename))
	ops.WriteString("old-name.txt")
	ops.WriteString("new-name.txt")

	reply := c.sendCompound(ops.Bytes(), 2)
	status, _ := c.parseCompoundReply(reply)
	require.Equal(t, uint32(NFS4_OK), status, "RENAME should succeed")

	// Verify: old name gone, new name present
	ops2 := &XDRWriter{}
	ops2.WriteUint32(uint32(OpPutRootFH))
	ops2.WriteUint32(uint32(OpReadDir))
	ops2.WriteUint64(0)
	ops2.WriteUint64(0)
	ops2.WriteUint32(4096)
	ops2.WriteUint32(4096)
	ops2.WriteUint32(0)

	reply2 := c.sendCompound(ops2.Bytes(), 2)
	status2, r := c.parseCompoundReply(reply2)
	require.Equal(t, uint32(NFS4_OK), status2)

	r.ReadUint32()
	r.ReadUint32()
	r.ReadUint32()
	r.ReadUint32()
	r.ReadUint64()

	var names []string
	for {
		follows, err := r.ReadUint32()
		if err != nil || follows == 0 {
			break
		}
		r.ReadUint64()
		name, _ := r.ReadString()
		r.ReadUint32()
		r.ReadOpaque()
		names = append(names, name)
	}

	assert.NotContains(t, names, "old-name.txt", "old name should be gone after RENAME")
	assert.Contains(t, names, "new-name.txt", "new name should appear after RENAME")
}

// TestE2E_Rename_NonExistent verifies that RENAME on a missing source returns NFS4ERR_NOENT.
func TestE2E_Rename_NonExistent(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	c := newNFS4Client(t, addr)

	ops := &XDRWriter{}
	ops.WriteUint32(uint32(OpPutRootFH))
	ops.WriteUint32(uint32(OpRename))
	ops.WriteString("does-not-exist.txt")
	ops.WriteString("also-nope.txt")

	reply := c.sendCompound(ops.Bytes(), 2)
	status, _ := c.parseCompoundReply(reply)
	assert.Equal(t, uint32(NFS4ERR_NOENT), status, "RENAME of non-existent file should return NOENT")
}
