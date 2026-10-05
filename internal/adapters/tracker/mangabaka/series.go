package mangabaka

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"

	"mangasync/internal/core"
	"mangasync/internal/httpx"
	"mangasync/internal/match"
)

var (
	_ core.Tracker       = (*Client)(nil)
	_ core.LibraryLister = (*Client)(nil)
)

type titleDTO struct {
	Title string `json:"title"`
}

type sourceDTO struct {
	ID json.RawMessage `json:"id"`
}

type seriesDTO struct {
	ID              int                   `json:"id"`
	State           string                `json:"state"`
	MergedWith      *int                  `json:"merged_with"`
	Title           string                `json:"title"`
	NativeTitle     *string               `json:"native_title"`
	RomanizedTitle  *string               `json:"romanized_title"`
	SecondaryTitles map[string][]titleDTO `json:"secondary_titles"`
	Status          *string               `json:"status"`
	Source          map[string]sourceDTO  `json:"source"`
}

var sourceKinds = map[string]core.IDKind{
	"anilist":            core.IDAniList,
	"my_anime_list":      core.IDMAL,
	"manga_updates":      core.IDMangaUpdates,
	"kitsu":              core.IDKitsu,
	"anime_planet":       core.IDAnimePlanet,
	"anime_news_network": core.IDANN,
}

// toCore maps a MangaBaka series. Alt titles: romanized, English secondary titles, other
// languages (sorted), then native — so Latin-script titles come first.
func (d seriesDTO) toCore() core.Series {
	id := strconv.Itoa(d.ID)
	s := core.Series{Ref: id, Title: d.Title, IDs: core.IDs{core.IDMangaBaka: id}}
	add := func(t string) {
		if t != "" && t != d.Title && !slices.Contains(s.AltTitles, t) {
			s.AltTitles = append(s.AltTitles, t)
		}
	}
	if d.RomanizedTitle != nil {
		add(*d.RomanizedTitle)
	}
	langs := make([]string, 0, len(d.SecondaryTitles))
	for l := range d.SecondaryTitles {
		langs = append(langs, l)
	}
	sort.Slice(langs, func(i, j int) bool {
		if (langs[i] == "en") != (langs[j] == "en") {
			return langs[i] == "en"
		}
		return langs[i] < langs[j]
	})
	for _, l := range langs {
		for _, t := range d.SecondaryTitles[l] {
			add(t.Title)
		}
	}
	if d.NativeTitle != nil {
		add(*d.NativeTitle)
	}
	for key, src := range d.Source {
		if kind, ok := sourceKinds[key]; ok {
			if v := rawID(src.ID); v != "" {
				s.IDs[kind] = v
			}
		}
	}
	return s
}

func rawID(r json.RawMessage) string {
	v := strings.Trim(strings.TrimSpace(string(r)), `"`)
	if v == "null" {
		return ""
	}
	return v
}

func (d seriesDTO) currentID() string {
	if d.State == "merged" && d.MergedWith != nil {
		return strconv.Itoa(*d.MergedWith)
	}
	return strconv.Itoa(d.ID)
}

func (c *Client) getSeries(ctx context.Context, id string) (seriesDTO, error) {
	var env struct {
		Data seriesDTO `json:"data"`
	}
	err := c.api.DoJSON(ctx, http.MethodGet, c.url("/v1/series/"+url.PathEscape(id)), c.header(), nil, &env)
	return env.Data, err
}

var sourcePaths = []struct {
	kind core.IDKind
	path string
}{
	{core.IDAniList, "anilist"},
	{core.IDMangaUpdates, "manga-updates"},
	{core.IDMAL, "my-anime-list"},
	{core.IDKitsu, "kitsu"},
	{core.IDAnimePlanet, "anime-planet"},
}

// Resolve: MangaBaka ID → cross-reference lookup → title search.
func (c *Client) Resolve(ctx context.Context, s core.Series) (string, bool, error) {
	if id := s.IDs[core.IDMangaBaka]; id != "" {
		return c.followMerged(ctx, id)
	}
	for _, sp := range sourcePaths {
		v := s.IDs[sp.kind]
		if v == "" {
			continue
		}
		var env struct {
			Data struct {
				Series []seriesDTO `json:"series"`
			} `json:"data"`
		}
		err := c.api.DoJSON(ctx, http.MethodGet, c.url("/v1/source/"+sp.path+"/"+url.PathEscape(v)), c.header(), nil, &env)
		if httpx.IsNotFound(err) {
			continue
		}
		if err != nil {
			return "", false, fmt.Errorf("source lookup %s/%s: %w", sp.path, v, err)
		}
		if len(env.Data.Series) > 0 {
			return env.Data.Series[0].currentID(), true, nil
		}
	}
	return c.searchTitle(ctx, s)
}

func (c *Client) followMerged(ctx context.Context, id string) (string, bool, error) {
	for range 3 {
		d, err := c.getSeries(ctx, id)
		if httpx.IsNotFound(err) {
			return "", false, nil
		}
		if err != nil {
			return "", false, err
		}
		next := d.currentID()
		if next == id {
			return id, true, nil
		}
		id = next
	}
	return id, true, nil
}

func (c *Client) searchTitle(ctx context.Context, s core.Series) (string, bool, error) {
	if s.Title == "" {
		return "", false, nil
	}
	q := url.Values{"q": {s.Title}, "limit": {"10"}}
	var env struct {
		Data []seriesDTO `json:"data"`
	}
	if err := c.search.DoJSON(ctx, http.MethodGet, c.url("/v1/series/search?"+q.Encode()), c.header(), nil, &env); err != nil {
		return "", false, fmt.Errorf("search %q: %w", s.Title, err)
	}
	best, _ := match.Best(s.Titles(), env.Data, func(d seriesDTO) []string { return d.toCore().Titles() }, c.cfg.Threshold)
	if best == nil {
		return "", false, nil
	}
	return best.Item.currentID(), true, nil
}

// SeriesEnded reports whether publication is completed or cancelled. Cached for EndedTTL.
func (c *Client) SeriesEnded(ctx context.Context, id string) (bool, error) {
	c.mu.Lock()
	e, ok := c.ended[id]
	c.mu.Unlock()
	if ok && c.now().Sub(e.at) < c.cfg.EndedTTL {
		return e.ended, nil
	}
	d, err := c.getSeries(ctx, id)
	if err != nil {
		return false, err
	}
	ended := d.Status != nil && (*d.Status == "completed" || *d.Status == "cancelled")
	c.mu.Lock()
	c.ended[id] = endedEntry{ended: ended, at: c.now()}
	c.mu.Unlock()
	return ended, nil
}
