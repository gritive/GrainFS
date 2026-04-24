package server

import "github.com/rs/zerolog"

// broadcastWriter fans each zerolog JSON line out to the SSE Hub
// so dashboard clients receive a live log stream.
type broadcastWriter struct {
	hub *Hub
}

func (bw *broadcastWriter) Write(p []byte) (int, error) {
	buf := make([]byte, len(p))
	copy(buf, p)
	bw.hub.Broadcast(Event{Type: "log", Data: buf})
	return len(p), nil
}

func (bw *broadcastWriter) WriteLevel(_ zerolog.Level, p []byte) (int, error) {
	return bw.Write(p)
}
