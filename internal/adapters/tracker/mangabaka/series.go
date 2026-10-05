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
	"unicode"

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

type v2TitleDTO struct {
	Language  string   `json:"language"`
	Traits    []string `json:"traits"`
	Title     string   `json:"title"`
	IsPrimary *bool    `json:"is_primary"`
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
	Titles          []v2TitleDTO          `json:"titles"`
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
	if s.Title == "" {
		s.Title = d.primaryV2Title() // v2 series have no top-level title
	}
	add := func(t string) {
		if t != "" && t != s.Title && !slices.Contains(s.AltTitles, t) {
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
	for _, t := range d.orderedV2Titles() {
		add(t)
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

// primaryV2Title picks the main title of a v2 series. MangaBaka marks one primary title per
// language, so priority is: primary English, any English, primary "-Latn" (romanized),
// any primary, first title.
func (d seriesDTO) primaryV2Title() string {
	primary := func(t v2TitleDTO) bool { return t.IsPrimary != nil && *t.IsPrimary }
	rules := []func(t v2TitleDTO) bool{
		func(t v2TitleDTO) bool { return primary(t) && t.Language == "en" },
		func(t v2TitleDTO) bool { return t.Language == "en" },
		func(t v2TitleDTO) bool { return primary(t) && strings.HasSuffix(t.Language, "-Latn") },
		primary,
		func(v2TitleDTO) bool { return true },
	}
	for _, rule := range rules {
		for _, t := range d.Titles {
			if t.Title != "" && rule(t) {
				return t.Title
			}
		}
	}
	return ""
}

// orderedV2Titles returns every v2 title: English first, then other Latin-script, then the rest.
func (d seriesDTO) orderedV2Titles() []string {
	var english, latin, other []string
	for _, t := range d.Titles {
		switch {
		case t.Title == "":
		case t.Language == "en":
			english = append(english, t.Title)
		case isLatin(t.Title):
			latin = append(latin, t.Title)
		default:
			other = append(other, t.Title)
		}
	}
	return slices.Concat(english, latin, other)
}

// isLatin reports whether every letter in s is Latin script.
func isLatin(s string) bool {
	for _, r := range s {
		if unicode.IsLetter(r) && !unicode.Is(unicode.Latin, r) {
			return false
		}
	}
	return true
}

func rawID(r json.RawMessage) string {
	v := strings.Trim(strings.TrimSpace(string(r)), `"`)
	if v == "null" {
		return ""
	}
	return v
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
		if httpx.IsNotFound(err) || httpx.IsStatus(err, http.StatusBadRequest) {
			continue
		}
		if err != nil {
			return "", false, fmt.Errorf("source lookup %s/%s: %w", sp.path, v, err)
		}
		if id, found, err := c.pickSourceSeries(ctx, env.Data.Series); err != nil || found {
			return id, found, err
		}
	}
	return c.searchTitle(ctx, s)
}

// pickSourceSeries returns the first active series; failing that, the first merged one
// followed to its target. Deleted series are skipped.
func (c *Client) pickSourceSeries(ctx context.Context, list []seriesDTO) (string, bool, error) {
	for _, d := range list {
		if d.State == "active" {
			return strconv.Itoa(d.ID), true, nil
		}
	}
	for _, d := range list {
		if d.State == "merged" && d.MergedWith != nil {
			if id, found, err := c.followMerged(ctx, strconv.Itoa(*d.MergedWith)); err != nil || found {
				return id, found, err
			}
		}
	}
	return "", false, nil
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
		switch {
		case d.State == "deleted":
			return "", false, nil
		case d.State != "merged":
			return id, true, nil
		case d.MergedWith == nil:
			return "", false, nil
		}
		id = strconv.Itoa(*d.MergedWith)
	}
	return "", false, nil // merge chain too long or cyclic
}

// searchTitle searches by the main title. A coloured edition ("Naruto (Color)") is also
// scored against, and if needed searched as, the original series ("Naruto").
func (c *Client) searchTitle(ctx context.Context, s core.Series) (string, bool, error) {
	if s.Title == "" {
		return "", false, nil
	}
	want := s.Titles()
	stripped, hasEdition := match.StripEdition(s.Title)
	if hasEdition {
		want = append(want, stripped)
	}
	if id, found, err := c.searchOnce(ctx, s.Title, want); err != nil || found || !hasEdition {
		return id, found, err
	}
	return c.searchOnce(ctx, stripped, want)
}

// searchOnce runs one title search and accepts the best candidate scoring against want.
func (c *Client) searchOnce(ctx context.Context, query string, want []string) (string, bool, error) {
	q := url.Values{"q": {query}, "limit": {"10"}}
	var env struct {
		Data []seriesDTO `json:"data"`
	}
	if err := c.search.DoJSON(ctx, http.MethodGet, c.url("/v1/series/search?"+q.Encode()), c.header(), nil, &env); err != nil {
		return "", false, fmt.Errorf("search %q: %w", query, err)
	}
	hits := slices.DeleteFunc(env.Data, func(d seriesDTO) bool { return d.State == "deleted" })
	best, _ := match.Best(want, hits, func(d seriesDTO) []string { return d.toCore().Titles() }, c.cfg.Threshold)
	if best == nil {
		return "", false, nil
	}
	if best.Item.State == "merged" {
		if best.Item.MergedWith == nil {
			return "", false, nil
		}
		return c.followMerged(ctx, strconv.Itoa(*best.Item.MergedWith))
	}
	return strconv.Itoa(best.Item.ID), true, nil
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
