package match

import (
	"net/url"
	"strings"

	"mangasync/internal/core"
)

// ParseLink extracts a cross-reference ID from a series URL. Unknown sites return ok=false.
func ParseLink(raw string) (core.IDKind, string, bool) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Hostname() == "" {
		return "", "", false
	}
	host := strings.TrimPrefix(strings.ToLower(u.Hostname()), "www.")
	seg := strings.Split(strings.Trim(u.Path, "/"), "/")
	at := func(i int) string {
		if i < len(seg) {
			return seg[i]
		}
		return ""
	}
	pathID := func(prefix string, kind core.IDKind) (core.IDKind, string, bool) {
		if at(0) == prefix && at(1) != "" {
			return kind, at(1), true
		}
		return "", "", false
	}
	numericPathID := func(prefix string, kind core.IDKind) (core.IDKind, string, bool) {
		if k, id, ok := pathID(prefix, kind); ok && isDigits(id) {
			return k, id, true
		}
		return "", "", false
	}
	switch host {
	case "mangabaka.org", "mangabaka.dev":
		return numericPathID("manga", core.IDMangaBaka)
	case "anilist.co":
		return numericPathID("manga", core.IDAniList)
	case "myanimelist.net":
		return numericPathID("manga", core.IDMAL)
	case "mangaupdates.com":
		return pathID("series", core.IDMangaUpdates)
	case "kitsu.app", "kitsu.io":
		return numericPathID("manga", core.IDKitsu)
	case "anime-planet.com":
		return pathID("manga", core.IDAnimePlanet)
	case "mangadex.org":
		return pathID("title", core.IDMangaDex)
	case "animenewsnetwork.com":
		if at(0) == "encyclopedia" && at(1) == "manga.php" {
			if id := u.Query().Get("id"); isDigits(id) {
				return core.IDANN, id, true
			}
		}
	}
	return "", "", false
}

// IDsFromLinks collects the IDs found in urls. The first URL of each kind wins.
func IDsFromLinks(urls []string) core.IDs {
	ids := core.IDs{}
	for _, raw := range urls {
		if kind, id, ok := ParseLink(raw); ok {
			if _, seen := ids[kind]; !seen {
				ids[kind] = id
			}
		}
	}
	return ids
}

// isDigits reports whether s is a non-empty string of ASCII digits.
func isDigits(s string) bool {
	return s != "" && strings.IndexFunc(s, func(r rune) bool { return r < '0' || r > '9' }) < 0
}
