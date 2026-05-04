package main

import "testing"

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		in   int64
		raw  bool
		want string
	}{
		{0, false, "0 B"},
		{512, false, "512 B"},
		{1024, false, "1.0 KiB"},
		{1 << 20, false, "1.0 MiB"},
		{1 << 30, false, "1.0 GiB"},
		{500 * (1 << 20), false, "500.0 MiB"},
		{1073741824, true, "1073741824"},
		{-1, false, "n/a"},
	}
	for _, tc := range tests {
		got := formatBytes(tc.in, tc.raw)
		if got != tc.want {
			t.Errorf("formatBytes(%d, %v) = %q, want %q", tc.in, tc.raw, got, tc.want)
		}
	}
}

func TestParseSize(t *testing.T) {
	tests := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		{"1024", 1024, false},
		{"1K", 1024, false},
		{"1Ki", 1024, false},
		{"1KiB", 1024, false},
		{"1M", 1 << 20, false},
		{"1G", 1 << 30, false},
		{"1Gi", 1 << 30, false},
		{"1.5G", 1610612736, false},
		{"100M", 100 * (1 << 20), false},
		{"", 0, true},
		{"abc", 0, true},
		{"1Z", 0, true}, // no Z suffix
	}
	for _, tc := range tests {
		got, err := parseSize(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseSize(%q) succeeded, want error", tc.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseSize(%q) err = %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("parseSize(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}
