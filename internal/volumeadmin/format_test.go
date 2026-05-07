package volumeadmin

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
		got := FormatBytes(tc.in, tc.raw)
		if got != tc.want {
			t.Errorf("FormatBytes(%d, %v) = %q, want %q", tc.in, tc.raw, got, tc.want)
		}
	}
}

func TestParseSize(t *testing.T) {
	tests := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		// bare bytes
		{"0", 0, false},
		{"1024", 1024, false},
		{"1024B", 1024, false},
		{"1b", 1, false},

		// IEC binary (Ki/Mi/Gi/Ti/Pi and KiB/MiB/...)
		{"1Ki", 1 << 10, false},
		{"1KiB", 1 << 10, false},
		{"1Mi", 1 << 20, false},
		{"1MiB", 1 << 20, false},
		{"1Gi", 1 << 30, false},
		{"1GiB", 1 << 30, false},
		{"1Ti", 1 << 40, false},
		{"1Pi", 1 << 50, false},
		{"1.5Gi", 1_610_612_736, false},
		{"100Mi", 100 * (1 << 20), false},

		// SI decimal (KB/MB/GB/TB/PB)
		{"1KB", 1_000, false},
		{"1MB", 1_000_000, false},
		{"1GB", 1_000_000_000, false},
		{"1TB", 1_000_000_000_000, false},
		{"1PB", 1_000_000_000_000_000, false},
		{"1.5GB", 1_500_000_000, false},
		{"500MB", 500_000_000, false},

		// case-insensitive
		{"1gib", 1 << 30, false},
		{"1KIB", 1 << 10, false},
		{"1gb", 1_000_000_000, false},
		{"1kb", 1_000, false},

		// ambiguous bare K/M/G/T/P → reject (Kubernetes convention split)
		{"1K", 0, true},
		{"1M", 0, true},
		{"1G", 0, true},
		{"1T", 0, true},
		{"1P", 0, true},
		{"1k", 0, true},
		{"1g", 0, true},
		{"1.5G", 0, true},

		// errors
		{"", 0, true},
		{"abc", 0, true},
		{"1Z", 0, true},
	}
	for _, tc := range tests {
		got, err := ParseSize(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ParseSize(%q) succeeded, want error", tc.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseSize(%q) err = %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ParseSize(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestFormatVolumeHealth(t *testing.T) {
	if got := FormatVolumeHealth(""); got != "ok" {
		t.Errorf("empty → %q want \"ok\"", got)
	}
	if got := FormatVolumeHealth("degraded"); got != "degraded" {
		t.Errorf("degraded → %q", got)
	}
}

func TestCapitalize(t *testing.T) {
	cases := map[string]string{"": "", "done": "Done", "ok": "Ok", "X": "X"}
	for in, want := range cases {
		if got := Capitalize(in); got != want {
			t.Errorf("Capitalize(%q) = %q want %q", in, got, want)
		}
	}
}
