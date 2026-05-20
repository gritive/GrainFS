package raft

import (
	"os"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Badger store exports", func() {
	var dir string
	var logPrefix []byte
	var stablePrefix []byte
	var snapPrefix []byte

	ginkgo.BeforeEach(func() {
		var err error
		dir, err = os.MkdirTemp("", "raft-store-exports-*")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(os.RemoveAll, dir)

		logPrefix = []byte("raft/v2/log/")
		stablePrefix = []byte("raft/v2/hardstate/")
		snapPrefix = []byte("raft/v2/snap/")
	})

	ginkgo.It("persists exported stores across reopen", func() {
		func() {
			db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer db.Close()

			ls, err := NewBadgerLogStore(db, logPrefix)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ss, err := NewBadgerStableStore(db, stablePrefix)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			_, err = NewBadgerSnapshotStore(db, snapPrefix)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			gomega.Expect(ls.Append([]LogEntry{
				{Term: 1, Index: 1, Type: LogEntryCommand, Command: []byte("alpha")},
				{Term: 1, Index: 2, Type: LogEntryCommand, Command: []byte("beta")},
				{Term: 2, Index: 3, Type: LogEntryCommand, Command: []byte("gamma")},
			})).To(gomega.Succeed())
			gomega.Expect(ss.SaveHardState(HardState{CurrentTerm: 7, VotedFor: "node-A"})).To(gomega.Succeed())
		}()

		db2, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(func() { _ = db2.Close() })

		ls2, err := NewBadgerLogStore(db2, logPrefix)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ss2, err := NewBadgerStableStore(db2, stablePrefix)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Expect(ls2.LastIndex()).To(gomega.Equal(uint64(3)), "log entries must survive reopen")
		e2, err := ls2.Entry(2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(e2.Command).To(gomega.Equal([]byte("beta")))

		hs, err := ss2.HardState()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(hs.CurrentTerm).To(gomega.Equal(uint64(7)), "HardState.CurrentTerm must survive reopen")
		gomega.Expect(hs.VotedFor).To(gomega.Equal("node-A"), "HardState.VotedFor must survive reopen")
	})

	ginkgo.It("restores NewNode state from durable exported stores", func() {
		open := func() (*Node, *badger.DB) {
			db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ls, err := NewBadgerLogStore(db, logPrefix)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ss, err := NewBadgerStableStore(db, stablePrefix)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			sn, err := NewBadgerSnapshotStore(db, snapPrefix)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			node, err := NewNode(Config{
				ID:            "node-A",
				LogStore:      ls,
				StableStore:   ss,
				SnapshotStore: sn,
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			return node, db
		}

		func() {
			db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer db.Close()
			ss, err := NewBadgerStableStore(db, stablePrefix)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(ss.SaveHardState(HardState{CurrentTerm: 9, VotedFor: "node-A"})).To(gomega.Succeed())
		}()

		node, db := open()
		ginkgo.DeferCleanup(func() { _ = db.Close() })
		gomega.Expect(node.Term()).To(gomega.Equal(uint64(9)), "Term must be restored from durable StableStore")
	})
})
