package match

import (
	"strings"

	"mangasync/internal/core"
)

// SameSeries reports whether a and b are the same series. A shared ID kind decides:
// equal IDs mean yes, different IDs mean no. Without a shared kind, titles must
// score at least threshold.
func SameSeries(a, b core.Series, threshold float64) bool {
	sharedKind := false
	for kind, va := range a.IDs {
		vb, ok := b.IDs[kind]
		if !ok || va == "" || vb == "" {
			continue
		}
		if strings.EqualFold(va, vb) {
			return true
		}
		sharedKind = true
	}
	if sharedKind {
		return false
	}
	return Score(a.Titles(), b.Titles()) >= threshold
}
