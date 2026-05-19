package server

import "strings"

type routeSurface uint8

const (
	routeSurfaceS3 routeSurface = iota
	routeSurfaceOps
	routeSurfaceAdmin
	routeSurfaceIceberg
	routeSurfaceUI
	routeSurfaceOAuth // OAuth2 token endpoint — carries its own credentials
)

type routeAuthnPolicy uint8

const (
	routeAuthnSigV4 routeAuthnPolicy = iota
	routeAuthnAnonymous
	routeAuthnLocalhost
)

type routeSurfaceEntry struct {
	pathPrefix  string
	pathExact   string
	surface     routeSurface
	authn       routeAuthnPolicy
	skipS3Authz bool
}

var routeSurfaceManifest = []routeSurfaceEntry{
	{pathExact: routePathMetrics, surface: routeSurfaceOps, authn: routeAuthnAnonymous, skipS3Authz: true},
	{pathPrefix: routePrefixAPI, surface: routeSurfaceOps, authn: routeAuthnSigV4},
	{pathPrefix: routePrefixUI, surface: routeSurfaceUI, authn: routeAuthnAnonymous, skipS3Authz: true},
	// OAuth2 token endpoint — carries its own credentials, no SigV4 required.
	{pathExact: routePrefixIceberg + routePathOAuthTokenSuffix, surface: routeSurfaceOAuth, authn: routeAuthnAnonymous, skipS3Authz: true},
	{pathExact: routePrefixIcebergAIStor + routePathOAuthTokenSuffix, surface: routeSurfaceOAuth, authn: routeAuthnAnonymous, skipS3Authz: true},
	{pathPrefix: routePrefixIceberg, surface: routeSurfaceIceberg, authn: routeAuthnSigV4, skipS3Authz: true},
	{pathPrefix: routePrefixIcebergAIStor, surface: routeSurfaceIceberg, authn: routeAuthnSigV4, skipS3Authz: true},
	{pathPrefix: routePrefixAdmin, surface: routeSurfaceAdmin, authn: routeAuthnLocalhost, skipS3Authz: true},
	{pathExact: routePathEvents, surface: routeSurfaceOps, authn: routeAuthnAnonymous},
	{pathExact: routePathHealEventsStream, surface: routeSurfaceOps, authn: routeAuthnAnonymous},
	{pathExact: routePathEventLog, surface: routeSurfaceOps, authn: routeAuthnAnonymous},
	{pathExact: routePathIncidents, surface: routeSurfaceOps, authn: routeAuthnAnonymous},
	{pathPrefix: routePrefixIncidents, surface: routeSurfaceOps, authn: routeAuthnAnonymous},
	{pathExact: routePathClusterStatus, surface: routeSurfaceOps, authn: routeAuthnAnonymous},
	{pathExact: routePathClusterRemovePeer, surface: routeSurfaceOps, authn: routeAuthnAnonymous},
	{pathExact: routePathAuditHealth, surface: routeSurfaceOps, authn: routeAuthnAnonymous},
	{pathExact: routePathAuditS3, surface: routeSurfaceOps, authn: routeAuthnAnonymous},
	{pathExact: routePathCacheStatus, surface: routeSurfaceOps, authn: routeAuthnAnonymous},
}

func routeAuthnPolicyForPath(path string) routeAuthnPolicy {
	if entry, ok := routeSurfaceEntryForPath(path); ok {
		return entry.authn
	}
	return routeAuthnSigV4
}

func routeSurfaceForPath(path string) routeSurface {
	if entry, ok := routeSurfaceEntryForPath(path); ok {
		return entry.surface
	}
	return routeSurfaceS3
}

func routeIsS3Path(path string) bool {
	return routeSurfaceForPath(path) == routeSurfaceS3
}

func routeIsUISurface(path string) bool {
	return path == routePathUI || routeSurfaceForPath(path) == routeSurfaceUI
}

func routeSkipsS3Authz(path string) bool {
	if path == routePathS3Root {
		return true
	}
	if entry, ok := routeSurfaceEntryForPath(path); ok {
		return entry.skipS3Authz
	}
	return false
}

func routeSurfaceEntryForPath(path string) (routeSurfaceEntry, bool) {
	// Exact match has highest priority; return immediately on first hit.
	for _, entry := range routeSurfaceManifest {
		if entry.pathExact != "" && path == entry.pathExact {
			return entry, true
		}
	}
	// Fall back to last prefix match (longer prefixes registered first take precedence).
	var matched routeSurfaceEntry
	var ok bool
	for _, entry := range routeSurfaceManifest {
		if entry.pathPrefix != "" && strings.HasPrefix(path, entry.pathPrefix) {
			matched = entry
			ok = true
		}
	}
	return matched, ok
}
