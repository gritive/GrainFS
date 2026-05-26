package transport

import (
	"strings"
	"sync"
	"testing"
)

func TestSwapIdentity_AtomicSnapshot(t *testing.T) {
	pskA := strings.Repeat("a", 64)
	pskB := strings.Repeat("b", 64)
	tr, err := NewQUICTransport(pskA)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()

	_, spkiB, err := DeriveClusterIdentity(pskB)
	if err != nil {
		t.Fatal(err)
	}
	multi := NewIdentitySnapshot([][32]byte{tr.expectedSPKI, spkiB}, tr.identityCert, tr.expectedSPKI)
	tr.SwapIdentity(multi)

	snap := tr.identity.Load()
	if len(snap.AcceptSPKIs) != 2 {
		t.Fatalf("want 2 accept SPKIs, got %d", len(snap.AcceptSPKIs))
	}
}

func TestPinAcceptedSPKI_AcceptsEither(t *testing.T) {
	pskA := strings.Repeat("a", 64)
	pskB := strings.Repeat("b", 64)
	certA, spkiA, _ := DeriveClusterIdentity(pskA)
	certB, spkiB, _ := DeriveClusterIdentity(pskB)

	snap := NewIdentitySnapshot([][32]byte{spkiA, spkiB}, certA, spkiA)
	pin := pinAcceptedSPKI(snap)

	if err := pin([][]byte{certA.Certificate[0]}, nil); err != nil {
		t.Fatalf("should accept A: %v", err)
	}
	if err := pin([][]byte{certB.Certificate[0]}, nil); err != nil {
		t.Fatalf("should accept B: %v", err)
	}
	cRand := generateRandomTLSCert(t)
	if err := pin([][]byte{cRand.Certificate[0]}, nil); err == nil {
		t.Fatal("should reject random cert")
	}
}

func TestSwapIdentity_ConcurrentReadDuringSwap(t *testing.T) {
	pskA := strings.Repeat("a", 64)
	pskB := strings.Repeat("b", 64)
	tr, err := NewQUICTransport(pskA)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	_, spkiB, _ := DeriveClusterIdentity(pskB)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				snap := tr.identity.Load()
				if len(snap.AcceptSPKIs) == 0 {
					t.Errorf("torn snapshot: empty AcceptSPKIs")
					return
				}
			}
		}
	}()

	for i := 0; i < 1000; i++ {
		tr.SwapIdentity(NewIdentitySnapshot([][32]byte{tr.expectedSPKI}, tr.identityCert, tr.expectedSPKI))
		tr.SwapIdentity(NewIdentitySnapshot([][32]byte{tr.expectedSPKI, spkiB}, tr.identityCert, tr.expectedSPKI))
	}
	close(stop)
	wg.Wait()
}
