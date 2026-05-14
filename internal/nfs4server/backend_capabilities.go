package nfs4server

import "github.com/gritive/GrainFS/internal/storage"

type backendUnwrapper interface {
	Unwrap() storage.Backend
}

func partialIOBackend(backend storage.Backend) (storage.PartialIO, bool) {
	for backend != nil {
		if partial, ok := backend.(storage.PartialIO); ok {
			return partial, true
		}
		unwrapper, ok := backend.(backendUnwrapper)
		if !ok {
			return nil, false
		}
		next := unwrapper.Unwrap()
		if next == backend {
			return nil, false
		}
		backend = next
	}
	return nil, false
}

func preferReadAt(backend storage.Backend, bucket string) bool {
	type readAtPreference interface {
		PreferReadAt(bucket string) bool
	}
	for backend != nil {
		if pref, ok := backend.(readAtPreference); ok {
			return pref.PreferReadAt(bucket)
		}
		unwrapper, ok := backend.(backendUnwrapper)
		if !ok {
			return true
		}
		next := unwrapper.Unwrap()
		if next == backend {
			return true
		}
		backend = next
	}
	return true
}

func preferWriteAt(backend storage.Backend, bucket string) bool {
	type writeAtPreference interface {
		PreferWriteAt(bucket string) bool
	}
	for backend != nil {
		if pref, ok := backend.(writeAtPreference); ok {
			return pref.PreferWriteAt(bucket)
		}
		unwrapper, ok := backend.(backendUnwrapper)
		if !ok {
			return true
		}
		next := unwrapper.Unwrap()
		if next == backend {
			return true
		}
		backend = next
	}
	return true
}

func truncatableBackend(backend storage.Backend) (storage.Truncatable, bool) {
	for backend != nil {
		if truncatable, ok := backend.(storage.Truncatable); ok {
			return truncatable, true
		}
		unwrapper, ok := backend.(backendUnwrapper)
		if !ok {
			return nil, false
		}
		next := unwrapper.Unwrap()
		if next == backend {
			return nil, false
		}
		backend = next
	}
	return nil, false
}
