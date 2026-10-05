package match

import (
	"regexp"
	"strings"
)

// editionSuffix matches a trailing bracketed colour-edition marker such as "(Color)",
// "[Colored]" or "(Official Colored Comics)".
var editionSuffix = regexp.MustCompile(`(?i)\s*[\(\[]\s*(?:(?:official|digital|full)\s+)?colou?r(?:ed)?(?:\s+(?:edition|version|comics?))?\s*[\)\]]\s*$`)

// StripEdition removes a trailing bracketed colour-edition marker from title, so a coloured
// edition ("Naruto (Color)") can match the original series ("Naruto"). It reports whether
// anything was stripped; the result is never empty.
func StripEdition(title string) (string, bool) {
	loc := editionSuffix.FindStringIndex(title)
	if loc == nil {
		return title, false
	}
	stripped := strings.TrimSpace(title[:loc[0]])
	if stripped == "" {
		return title, false
	}
	return stripped, true
}
