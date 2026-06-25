package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseCopySourceRange(t *testing.T) {
	const size = 100
	tests := []struct {
		name      string
		raw       string
		size      int64
		wantStart int64
		wantEnd   int64
		wantOK    bool
	}{
		{name: "whole-prefix range", raw: "bytes=0-99", size: size, wantStart: 0, wantEnd: 99, wantOK: true},
		{name: "mid range", raw: "bytes=10-49", size: size, wantStart: 10, wantEnd: 49, wantOK: true},
		{name: "single byte", raw: "bytes=5-5", size: size, wantStart: 5, wantEnd: 5, wantOK: true},
		{name: "missing bytes= prefix", raw: "0-99", size: size, wantOK: false},
		{name: "no dash", raw: "bytes=10", size: size, wantOK: false},
		{name: "non-numeric start", raw: "bytes=a-9", size: size, wantOK: false},
		{name: "non-numeric end", raw: "bytes=0-z", size: size, wantOK: false},
		{name: "negative start", raw: "bytes=-1-9", size: size, wantOK: false},
		{name: "start greater than end", raw: "bytes=9-1", size: size, wantOK: false},
		{name: "end at size is out of range", raw: "bytes=0-100", size: size, wantOK: false},
		{name: "end past size", raw: "bytes=0-200", size: size, wantOK: false},
		{name: "empty", raw: "", size: size, wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end, ok := parseCopySourceRange(tt.raw, tt.size)
			assert.Equal(t, tt.wantOK, ok)
			if tt.wantOK {
				assert.Equal(t, tt.wantStart, start)
				assert.Equal(t, tt.wantEnd, end)
			}
		})
	}
}
