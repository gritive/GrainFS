package server

import (
	"io"
	"os"
	"testing"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func TestMain(m *testing.M) {
	broadcastLoggerEnabled = false
	hlog.SetOutput(io.Discard)
	log.Logger = zerolog.New(io.Discard)
	os.Exit(m.Run())
}
