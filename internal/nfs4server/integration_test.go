package nfs4server

import (
	"context"
	"net"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

type nfsTestTB interface {
	Helper()
	Cleanup(func())
	TempDir() string
	Errorf(format string, args ...interface{})
	FailNow()
}

func startTestNFS4Server(t nfsTestTB) (string, *Server) {
	t.Helper()

	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)

	// Create the NFS4 bucket
	require.NoError(t, backend.CreateBucket(context.Background(), legacyNFS4Bucket))

	srv := NewServer(backend)
	srv.SetExportsForTest(buildSnap(map[string]exportConfig{
		legacyNFS4Bucket: {fsidMajor: 1, fsidMinor: 1, generation: 1},
	}))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv.mu.Lock()
	srv.listener = ln
	srv.mu.Unlock()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.handleConn(conn)
		}
	}()

	t.Cleanup(func() { srv.Close() })
	return ln.Addr().String(), srv
}

var _ = Describe("NFS4 integration", func() {
	var (
		addr string
		t    nfsTestTB
	)

	BeforeEach(func() {
		t = GinkgoT()
		addr, _ = startTestNFS4Server(t)
	})

	dial := func() net.Conn {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(conn.Close)
		return conn
	}

	It("handles the NULL procedure", func() {
		conn := dial()

		w := &XDRWriter{}
		w.WriteUint32(1)
		w.WriteUint32(rpcMsgCall)
		w.WriteUint32(2)
		w.WriteUint32(rpcProgNFS)
		w.WriteUint32(rpcVersNFS4)
		w.WriteUint32(0)
		w.WriteUint32(authNone)
		w.WriteUint32(0)
		w.WriteUint32(authNone)
		w.WriteUint32(0)

		Expect(writeRPCFrame(conn, w.Bytes())).To(Succeed())

		reply, err := readRPCFrame(conn)
		Expect(err).NotTo(HaveOccurred())
		Expect(len(reply)).To(BeNumerically(">=", 24), "reply should have RPC header")

		r := NewXDRReader(reply)
		xid, _ := r.ReadUint32()
		Expect(xid).To(Equal(uint32(1)))
	})

	It("handles PUTROOTFH and GETFH compounds", func() {
		conn := dial()

		compound := &XDRWriter{}
		compound.WriteString("")
		compound.WriteUint32(0)
		compound.WriteUint32(2)
		compound.WriteUint32(uint32(OpPutRootFH))
		compound.WriteUint32(uint32(OpGetFH))

		rpc := &XDRWriter{}
		rpc.WriteUint32(2)
		rpc.WriteUint32(rpcMsgCall)
		rpc.WriteUint32(2)
		rpc.WriteUint32(rpcProgNFS)
		rpc.WriteUint32(rpcVersNFS4)
		rpc.WriteUint32(1)
		rpc.WriteUint32(authNone)
		rpc.WriteUint32(0)
		rpc.WriteUint32(authNone)
		rpc.WriteUint32(0)
		rpc.buf.Write(compound.Bytes())

		Expect(writeRPCFrame(conn, rpc.Bytes())).To(Succeed())

		reply, err := readRPCFrame(conn)
		Expect(err).NotTo(HaveOccurred())

		r := NewXDRReader(reply)
		xid, _ := r.ReadUint32()
		Expect(xid).To(Equal(uint32(2)))

		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()
		r.ReadOpaque()
		r.ReadUint32()

		status, _ := r.ReadUint32()
		Expect(status).To(Equal(uint32(NFS4_OK)))

		tag, _ := r.ReadString()
		Expect(tag).To(Equal(""))

		opCount, _ := r.ReadUint32()
		Expect(opCount).To(Equal(uint32(2)))

		op1Code, _ := r.ReadUint32()
		Expect(op1Code).To(Equal(uint32(OpPutRootFH)))
		op1Status, _ := r.ReadUint32()
		Expect(op1Status).To(Equal(uint32(NFS4_OK)))

		op2Code, _ := r.ReadUint32()
		Expect(op2Code).To(Equal(uint32(OpGetFH)))
		op2Status, _ := r.ReadUint32()
		Expect(op2Status).To(Equal(uint32(NFS4_OK)))

		fh, _ := r.ReadOpaque()
		Expect(fh).To(HaveLen(16), "filehandle should be 16 bytes")
	})

	It("handles SETCLIENTID compounds", func() {
		conn := dial()

		compound := &XDRWriter{}
		compound.WriteString("")
		compound.WriteUint32(0)
		compound.WriteUint32(1)
		compound.WriteUint32(uint32(OpSetClientID))
		compound.WriteUint64(12345)
		compound.WriteString("test-client")
		compound.WriteUint32(0)
		compound.WriteString("tcp")
		compound.WriteString("127.0.0.1.0.0")
		compound.WriteUint32(0)

		rpc := &XDRWriter{}
		rpc.WriteUint32(3)
		rpc.WriteUint32(rpcMsgCall)
		rpc.WriteUint32(2)
		rpc.WriteUint32(rpcProgNFS)
		rpc.WriteUint32(rpcVersNFS4)
		rpc.WriteUint32(1)
		rpc.WriteUint32(authNone)
		rpc.WriteUint32(0)
		rpc.WriteUint32(authNone)
		rpc.WriteUint32(0)
		rpc.buf.Write(compound.Bytes())

		Expect(writeRPCFrame(conn, rpc.Bytes())).To(Succeed())

		reply, err := readRPCFrame(conn)
		Expect(err).NotTo(HaveOccurred())

		r := NewXDRReader(reply)
		xid, _ := r.ReadUint32()
		Expect(xid).To(Equal(uint32(3)))

		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()
		r.ReadOpaque()
		r.ReadUint32()

		status, _ := r.ReadUint32()
		Expect(status).To(Equal(uint32(NFS4_OK)))
	})

	It("writes and reads a file", func() {
		conn := dial()

		compound := &XDRWriter{}
		compound.WriteString("")
		compound.WriteUint32(0)
		compound.WriteUint32(5)

		compound.WriteUint32(uint32(OpPutRootFH))
		writeLookupLegacyExport(compound)

		compound.WriteUint32(uint32(OpOpen))
		compound.WriteUint32(0)
		compound.WriteUint32(2)
		compound.WriteUint32(0)
		compound.WriteUint64(1)
		compound.WriteString("owner-1")
		compound.WriteUint32(1)
		compound.WriteUint32(0)
		compound.WriteUint32(0)
		compound.WriteOpaque(nil)
		compound.WriteUint32(0)
		compound.WriteString("test.txt")

		compound.WriteUint32(uint32(OpWrite))
		compound.WriteUint32(1)
		compound.WriteUint64(0)
		compound.WriteUint32(0)
		compound.WriteUint64(0)
		compound.WriteUint32(2)
		compound.WriteOpaque([]byte("hello nfs4!"))

		compound.WriteUint32(uint32(OpClose))
		compound.WriteUint32(0)
		compound.WriteUint32(0)
		compound.WriteUint64(0)
		compound.WriteUint32(0)

		rpc := buildRPCCallFrame(4, compound.Bytes())
		Expect(writeRPCFrame(conn, rpc)).To(Succeed())

		reply, err := readRPCFrame(conn)
		Expect(err).NotTo(HaveOccurred())

		r := NewXDRReader(reply)
		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()
		r.ReadOpaque()
		r.ReadUint32()

		status, _ := r.ReadUint32()
		Expect(status).To(Equal(uint32(NFS4_OK)), "write compound should succeed")

		compound2 := &XDRWriter{}
		compound2.WriteString("")
		compound2.WriteUint32(0)
		compound2.WriteUint32(4)

		compound2.WriteUint32(uint32(OpPutRootFH))
		writeLookupLegacyExport(compound2)
		writeLookupFile(compound2, "test.txt")

		compound2.WriteUint32(uint32(OpRead))
		compound2.WriteUint32(0)
		compound2.WriteUint64(0)
		compound2.WriteUint32(0)
		compound2.WriteUint64(0)
		compound2.WriteUint32(1024)

		rpc2 := buildRPCCallFrame(5, compound2.Bytes())
		Expect(writeRPCFrame(conn, rpc2)).To(Succeed())

		reply2, err := readRPCFrame(conn)
		Expect(err).NotTo(HaveOccurred())

		r2 := NewXDRReader(reply2)
		r2.ReadUint32()
		r2.ReadUint32()
		r2.ReadUint32()
		r2.ReadUint32()
		r2.ReadOpaque()
		r2.ReadUint32()

		status2, _ := r2.ReadUint32()
		Expect(status2).To(Equal(uint32(NFS4_OK)), "read compound should succeed")

		r2.ReadString()
		opCount, _ := r2.ReadUint32()
		Expect(opCount).To(Equal(uint32(4)))

		r2.ReadUint32()
		r2.ReadUint32()
		r2.ReadUint32()
		r2.ReadUint32()
		r2.ReadUint32()
		r2.ReadUint32()
		readOpCode, _ := r2.ReadUint32()
		Expect(readOpCode).To(Equal(uint32(OpRead)))
		readStatus, _ := r2.ReadUint32()
		Expect(readStatus).To(Equal(uint32(NFS4_OK)))

		r2.ReadUint32()
		readData, err := r2.ReadOpaque()
		Expect(err).NotTo(HaveOccurred())
		Expect(string(readData)).To(Equal("hello nfs4!"))
	})
})

func buildRPCCallFrame(xid uint32, compoundData []byte) []byte {
	rpc := &XDRWriter{}
	rpc.WriteUint32(xid)
	rpc.WriteUint32(rpcMsgCall)
	rpc.WriteUint32(2)
	rpc.WriteUint32(rpcProgNFS)
	rpc.WriteUint32(rpcVersNFS4)
	rpc.WriteUint32(1) // COMPOUND
	rpc.WriteUint32(authNone)
	rpc.WriteUint32(0)
	rpc.WriteUint32(authNone)
	rpc.WriteUint32(0)
	rpc.buf.Write(compoundData)
	return rpc.Bytes()
}
