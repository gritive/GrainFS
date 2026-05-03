package nfs4server

import "errors"

var errInvalidComponent = errors.New("invalid component name")

// validateComponentName rejects component names that RFC 7530 §6 declares
// illegal: empty, ".", and "..". Used by every op handler that consumes a
// component name from XDR (LOOKUP, OPEN, CREATE, LINK, REMOVE, RENAME).
func validateComponentName(name string) error {
	if name == "" || name == "." || name == ".." {
		return errInvalidComponent
	}
	return nil
}
