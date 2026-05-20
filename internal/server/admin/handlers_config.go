package admin

import (
	"context"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/config"
)

// ConfigSetEntry sets a cluster-wide config key via the proposer.
func ConfigSetEntry(ctx context.Context, d *Deps, key, value string) error {
	return d.ConfigProposer.ProposeConfigPut(ctx, key, value)
}

// ConfigUnsetEntry removes a cluster-wide config key override via the proposer.
func ConfigUnsetEntry(ctx context.Context, d *Deps, key string) error {
	return d.ConfigProposer.ProposeConfigDelete(ctx, key)
}

// ConfigGetEntry returns the current value for a single config key.
func ConfigGetEntry(_ context.Context, d *Deps, key string) (adminapi.ConfigEntry, error) {
	entries := d.ConfigStore.ListAll()
	for _, e := range entries {
		if e.Key == key {
			return toConfigEntry(e), nil
		}
	}
	return adminapi.ConfigEntry{}, NewNotFound("unknown config key: " + key)
}

// ConfigListEntries returns the config entries. When all is false, only entries
// with Set=true are returned. When all is true, the full registered catalog is
// returned including description.
func ConfigListEntries(_ context.Context, d *Deps, all bool) ([]adminapi.ConfigEntry, error) {
	entries := d.ConfigStore.ListAll()
	out := make([]adminapi.ConfigEntry, 0, len(entries))
	for _, e := range entries {
		if !all && !e.Set {
			continue
		}
		out = append(out, toConfigEntry(e))
	}
	return out, nil
}

func toConfigEntry(e config.Entry) adminapi.ConfigEntry {
	return adminapi.ConfigEntry{
		Key:         e.Key,
		Value:       e.Value,
		Kind:        e.Kind,
		Default:     e.Default,
		Set:         e.Set,
		Description: e.Description,
	}
}
