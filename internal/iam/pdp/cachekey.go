package pdp

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
)

// cacheKey returns the hex SHA-256 of a canonical JSON encoding of the wire
// Request. It EXCLUDES RequestID (per-request nonce) but INCLUDES SchemaVersion,
// and normalizes nil==empty: groups are sorted (order-insensitive), and context
// entries with empty values are treated as absent.
func cacheKey(req Request) string {
	groups := make([]string, 0, len(req.Principal.Groups))
	groups = append(groups, req.Principal.Groups...)
	sort.Strings(groups) // order-insensitive; make([]string,0) ⇒ marshals [] not null
	type canonPrincipal struct {
		Kind, ID, Issuer, Subject, CredentialID string
		Groups                                  []string
	}
	cp := canonPrincipal{req.Principal.Kind, req.Principal.ID, req.Principal.Issuer, req.Principal.Subject, req.Principal.CredentialID, groups}
	keys := make([]string, 0, len(req.Context))
	for k, v := range req.Context {
		if v != "" {
			keys = append(keys, k)
		}
	} // empty value ⇒ treated as absent
	sort.Strings(keys)
	type kv struct{ K, V string }
	cx := make([]kv, 0, len(keys))
	for _, k := range keys {
		cx = append(cx, kv{k, req.Context[k]})
	}
	canon := struct {
		SchemaVersion              int
		Principal                  canonPrincipal
		Action, Resource, Protocol string
		Context                    []kv
	}{req.SchemaVersion, cp, req.Action, req.Resource, req.Protocol, cx}
	b, _ := json.Marshal(canon)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
