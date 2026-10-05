package match

import (
	"net/url"
	"strings"

	"mangasync/internal/core"
)

// ParseLink extracts a cross-reference ID from a series URL. Unknown sites return ok=false.
func ParseLink(raw string) (core.IDKind, string, bool) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" {
		return "", "", false
	}
	host := strings.TrimPrefix(strings.ToLower(u.Host), "www.")
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
	switch host {
	case "mangabaka.org", "mangabaka.dev":
		return pathID("manga", core.IDMangaBaka)
	case "anilist.co":
		return pathID("manga", core.IDAniList)
	case "myanimelist.net":
		return pathID("manga", core.IDMAL)
	case "mangaupdates.com":
		return pathID("series", core.IDMangaUpdates)
	case "kitsu.app", "kitsu.io":
		return pathID("manga", core.IDKitsu)
	case "anime-planet.com":
		return pathID("manga", core.IDAnimePlanet)
	case "mangadex.org":
		return pathID("title", core.IDMangaDex)
	case "animenewsnetwork.com":
		if at(0) == "encyclopedia" && at(1) == "manga.php" {
			if id := u.Query().Get("id"); id != "" {
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
