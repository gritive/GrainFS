package server

import (
	"fmt"
	"io"
)

func writeSSEOpen(w io.Writer) {
	fmt.Fprint(w, ": ok\n\n") //nolint:errcheck
}

func writeSSEKeepAlive(w io.Writer) {
	fmt.Fprint(w, ": keep-alive\n\n") //nolint:errcheck
}

func writeSSEEvent(w io.Writer, e Event) {
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", e.Type, e.Data) //nolint:errcheck
}
