package volumeadmin

import (
	"strings"
)

// Capitalize returns s with its first byte upper-cased. Used by scrub status
// output ("Done. ..." vs "done. ...").
func Capitalize(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}
