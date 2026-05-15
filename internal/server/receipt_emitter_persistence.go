package server

import (
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/receipt"
	"github.com/gritive/GrainFS/internal/scrubber"
)

func persistHealReceipt(store *receipt.Store, keyStore *receipt.KeyStore, correlationID string, events []scrubber.HealEvent) {
	r := buildReceipt(correlationID, events)
	if err := receipt.Sign(r, keyStore); err != nil {
		log.Warn().Str("correlation_id", correlationID).Err(err).Msg("receipt: sign failed, session not persisted")
		return
	}
	if err := store.Put(r); err != nil {
		log.Warn().Str("correlation_id", correlationID).Str("receipt_id", r.ReceiptID).Err(err).Msg("receipt: store failed")
	}
}
