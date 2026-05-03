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
	multi := &IdentitySnapshot{
		AcceptSPKIs: [][32]byte{tr.expectedSPKI, spkiB},
		PresentCert: tr.identityCert,
		PresentSPKI: tr.expectedSPKI,
	}
	tr.SwapIdentity(multi)

	snap := tr.identity.Load()
	if len(snap.AcceptSPKIs) != 2 {
		t.Fatalf("want 2 accept SPKIs, got %d", len(snap.AcceptSPKIs))
	}
}

func TestPinAnyAcceptedSPKI_AcceptsEither(t *testing.T) {
	pskA := strings.Repeat("a", 64)
	pskB := strings.Repeat("b", 64)
	certA, spkiA, _ := DeriveClusterIdentity(pskA)
	certB, spkiB, _ := DeriveClusterIdentity(pskB)

	pin := pinAnyAcceptedSPKI([][32]byte{spkiA, spkiB})

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
		tr.SwapIdentity(&IdentitySnapshot{
			AcceptSPKIs: [][32]byte{tr.expectedSPKI},
			PresentCert: tr.identityCert,
			PresentSPKI: tr.expectedSPKI,
		})
		tr.SwapIdentity(&IdentitySnapshot{
			AcceptSPKIs: [][32]byte{tr.expectedSPKI, spkiB},
			PresentCert: tr.identityCert,
			PresentSPKI: tr.expectedSPKI,
		})
	}
	close(stop)
	wg.Wait()
}
