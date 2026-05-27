package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCredentialCommandHelp(t *testing.T) {
	var out bytes.Buffer
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetArgs([]string{"credential", "--help"})
	t.Cleanup(func() {
		rootCmd.SetArgs(nil)
		rootCmd.SetOut(nil)
		rootCmd.SetErr(nil)
	})

	require.NoError(t, rootCmd.Execute())
	require.Contains(t, out.String(), "create")
	require.Contains(t, out.String(), "rotate")
	require.Contains(t, out.String(), "--endpoint")
}
