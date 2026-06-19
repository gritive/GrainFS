package cluster

// soleAuthTransitionAllowed reports whether the one-way soleauth guard permits
// the transition from → to. The allowed transitions are:
//
//	same → same  (idempotent)
//	off  → pending
//	pending → on
//	pending → off  (abort)
//
// Everything else is refused: off→on (must step through pending),
// on→pending and on→off (on is terminal), and any unknown state string.
func soleAuthTransitionAllowed(from, to string) bool {
	if from == to {
		return true
	}
	switch from {
	case soleAuthOff:
		return to == soleAuthPending
	case soleAuthPending:
		return to == soleAuthOn || to == soleAuthOff
	}
	return false
}
