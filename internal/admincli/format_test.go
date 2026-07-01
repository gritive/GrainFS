package admincli

import "testing"

func TestCapitalize(t *testing.T) {
	cases := map[string]string{"": "", "done": "Done", "ok": "Ok", "X": "X"}
	for in, want := range cases {
		if got := Capitalize(in); got != want {
			t.Errorf("Capitalize(%q) = %q want %q", in, got, want)
		}
	}
}
