package nfs4server

import (
	"errors"
	"strings"
)

var errInvalidComponent = errors.New("invalid component name")

// validateComponentName rejects component names that RFC 7530 §6 declares
// illegal:
//   - empty string
//   - "." or ".."
//   - any name containing "/" (component4 is a single path segment;
//     '/' would let a client like LOOKUP("../foo") traverse out of the
//     intended directory because path.Join cleans dotdot segments)
//
// Used by every op handler that consumes a component name from XDR
// (LOOKUP, OPEN, CREATE, LINK, REMOVE, RENAME).
func validateComponentName(name string) error {
	if name == "" || name == "." || name == ".." {
		return errInvalidComponent
	}
	if strings.ContainsRune(name, '/') {
		return errInvalidComponent
	}
	return nil
}
