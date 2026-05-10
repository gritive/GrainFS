package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadSecretKey_Stdin(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{"plain", "secret", "secret"},
		{"trailing LF", "secret\n", "secret"},
		{"trailing CRLF", "secret\r\n", "secret"},
		{"multi-line takes first", "secret\nignored", "secret"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := readSecretKey(true, "", strings.NewReader(c.input))
			if err != nil {
				t.Fatalf("readSecretKey: %v", err)
			}
			if got != c.want {
				t.Errorf("got %q want %q", got, c.want)
			}
		})
	}
}

func TestReadSecretKey_StdinClosed(t *testing.T) {
	got, err := readSecretKey(true, "", strings.NewReader(""))
	if err == nil {
		t.Fatalf("readSecretKey on empty stdin: want error, got %q", got)
	}
	if !strings.Contains(err.Error(), "stdin") {
		t.Errorf("error should mention stdin: %v", err)
	}
}

func TestReadSecretKey_File(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name    string
		content string
		want    string
	}{
		{"plain", "secret", "secret"},
		{"trailing newline", "secret\n", "secret"},
		{"leading + trailing whitespace", "  secret  \n", "secret"},
		{"tabs and CRLF", "\tsecret\r\n", "secret"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(dir, c.name+".txt")
			if err := os.WriteFile(path, []byte(c.content), 0o600); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}
			got, err := readSecretKey(false, path, nil)
			if err != nil {
				t.Fatalf("readSecretKey: %v", err)
			}
			if got != c.want {
				t.Errorf("got %q want %q", got, c.want)
			}
		})
	}
}

func TestReadSecretKey_FileNotFound(t *testing.T) {
	_, err := readSecretKey(false, "/nonexistent/path/should/not/exist", nil)
	if err == nil {
		t.Fatal("readSecretKey on missing file: want error, got nil")
	}
}
