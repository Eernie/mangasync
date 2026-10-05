// Package envutil reads configuration from environment-style lookups.
package envutil

import "strings"

// Lookup has the signature of os.LookupEnv.
type Lookup func(key string) (string, bool)

// Get returns the trimmed value of key, or "" if unset.
func Get(l Lookup, key string) string {
	v, _ := l(key)
	return strings.TrimSpace(v)
}

// GetOr returns the trimmed value of key if it is set (even to ""), else def.
func GetOr(l Lookup, key, def string) string {
	if v, ok := l(key); ok {
		return strings.TrimSpace(v)
	}
	return def
}

// List splits a comma-separated value, trimming items and dropping empty ones.
func List(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// MapLookup adapts a map for tests.
func MapLookup(m map[string]string) Lookup {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}
