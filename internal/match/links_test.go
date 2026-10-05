package match

import (
	"testing"

	"mangasync/internal/core"
)

func TestParseLink(t *testing.T) {
	cases := []struct {
		url  string
		kind core.IDKind
		id   string
		ok   bool
	}{
		{"https://www.mangaupdates.com/series/ylx5wzn/chainsaw-man", core.IDMangaUpdates, "ylx5wzn", true},
		{"https://anilist.co/manga/105778", core.IDAniList, "105778", true},
		{"https://anilist.co/manga/105778/Chainsaw-Man/", core.IDAniList, "105778", true},
		{"https://mangadex.org/title/a77742b1-befd-49a4-bff5-1ad4e6b0ef7b", core.IDMangaDex, "a77742b1-befd-49a4-bff5-1ad4e6b0ef7b", true},
		{"https://www.anime-planet.com/manga/chainsaw-man", core.IDAnimePlanet, "chainsaw-man", true},
		{"https://kitsu.app/manga/54139", core.IDKitsu, "54139", true},
		{"https://kitsu.io/manga/54139", core.IDKitsu, "54139", true},
		{"https://myanimelist.net/manga/116778", core.IDMAL, "116778", true},
		{"https://mangabaka.org/manga/1677/Chainsaw-Man", core.IDMangaBaka, "1677", true},
		{"https://www.animenewsnetwork.com/encyclopedia/manga.php?id=21271", core.IDANN, "21271", true},
		{"https://www.amazon.co.jp/dp/B07MX551PW", "", "", false},
		{"https://anilist.co/user/someone", "", "", false},
		{"not a url", "", "", false},
		{"https://anilist.co:443/manga/1", core.IDAniList, "1", true},
		{"https://kitsu.app/manga/chainsaw-man", "", "", false},
		{"https://anilist.co/manga/abc", "", "", false},
		{"https://myanimelist.net/manga/12x", "", "", false},
		{"https://mangabaka.org/manga/chainsaw-man", "", "", false},
		{"https://www.animenewsnetwork.com/encyclopedia/manga.php?id=abc", "", "", false},
	}
	for _, c := range cases {
		kind, id, ok := ParseLink(c.url)
		if kind != c.kind || id != c.id || ok != c.ok {
			t.Errorf("ParseLink(%q) = %q, %q, %v; want %q, %q, %v", c.url, kind, id, ok, c.kind, c.id, c.ok)
		}
	}
}

func TestIDsFromLinksFirstWins(t *testing.T) {
	ids := IDsFromLinks([]string{
		"https://anilist.co/manga/1",
		"https://www.amazon.co.jp/dp/X",
		"https://anilist.co/manga/2",
		"https://myanimelist.net/manga/3",
	})
	if len(ids) != 2 || ids[core.IDAniList] != "1" || ids[core.IDMAL] != "3" {
		t.Fatalf("got %v", ids)
	}
}
