package serveruntime

import (
	"context"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/encrypt"
)

func TestWaitDEKReady_ReadyImmediatelyWhenGenInstalled(t *testing.T) {
	kek := make([]byte, encrypt.KEKSize)
	k, err := encrypt.NewDEKKeeper(kek, restoreTestClusterID()) // gen-0 present
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := WaitDEKReady(ctx, k); err != nil {
		t.Fatalf("genesis keeper must be ready immediately: %v", err)
	}
}

func TestWaitDEKReady_BlocksThenReadyAfterInstall(t *testing.T) {
	kek := make([]byte, encrypt.KEKSize)
	cid := restoreTestClusterID()
	leader, err := encrypt.NewDEKKeeper(kek, cid)
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	lv, _ := leader.VersionsAndActive()
	k, err := encrypt.NewEmptyDEKKeeper(kek, cid)
	if err != nil {
		t.Fatalf("NewEmptyDEKKeeper: %v", err)
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = k.InstallReplicatedDEK(0, lv[0], 0)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := WaitDEKReady(ctx, k); err != nil {
		t.Fatalf("must become ready after install: %v", err)
	}
}

func TestWaitDEKReady_DeadlineExceededWhenNeverInstalled(t *testing.T) {
	kek := make([]byte, encrypt.KEKSize)
	k, err := encrypt.NewEmptyDEKKeeper(kek, restoreTestClusterID())
	if err != nil {
		t.Fatalf("NewEmptyDEKKeeper: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := WaitDEKReady(ctx, k); err == nil {
		t.Fatal("must return ctx error when DEK never installs (no deadlock)")
	}
}

func TestWaitDEKReady_NilKeeperReturnsNil(t *testing.T) {
	ctx := context.Background()
	if err := WaitDEKReady(ctx, nil); err != nil {
		t.Fatalf("nil keeper (encryption disabled) must return nil: %v", err)
	}
}
