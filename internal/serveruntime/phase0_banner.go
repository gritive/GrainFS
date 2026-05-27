package serveruntime

import "github.com/gritive/GrainFS/internal/server"

// bootPhase0Banner emits the default bucket anonymous-access startup banner.
func bootPhase0Banner(state *bootState) error {
	if state == nil || state.bannerWriter == nil {
		return nil
	}
	server.EmitBanner(state.bannerWriter, false)
	return nil
}
