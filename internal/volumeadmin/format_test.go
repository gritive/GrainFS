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
