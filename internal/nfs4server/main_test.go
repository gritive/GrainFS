package nfs4server

import (
	"runtime"
	"testing"
)

func TestMain(m *testing.M) {
	runtime.SetMutexProfileFraction(1)
	m.Run()
}
