// Package komga is the Reader adapter for Komga (REST API + SSE events).
package komga

import (
	"context"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"mangasync/internal/core"
	"mangasync/internal/httpx"
	"mangasync/internal/match"
)

var (
	_ core.Reader          = (*Client)(nil)
	_ core.ProgressWatcher = (*Client)(nil)
)

type Config struct {
	URL             string
	APIKey          string
	VolumeLibraries []string // library IDs whose books are volumes
}

type Client struct {
	cfg    Config
	api    *httpx.Client
	stream *http.Client // no timeout: SSE connections stay open
}

func New(cfg Config, api *httpx.Client) *Client {
	cfg.URL = strings.TrimRight(cfg.URL, "/")
	return &Client{cfg: cfg, api: api, stream: &http.Client{}}
}

func (c *Client) Name() string { return "komga" }

func (c *Client) header() http.Header {
	h := http.Header{}
	h.Set("X-API-Key", c.cfg.APIKey)
	return h
}

type seriesDTO struct {
	ID        string `json:"id"`
	LibraryID string `json:"libraryId"`
	Name      string `json:"name"`
	Deleted   bool   `json:"deleted"` // soft-deleted: files are gone
	Metadata  struct {
		Title           string `json:"title"`
		AlternateTitles []struct {
			Title string `json:"title"`
		} `json:"alternateTitles"`
		Links []struct {
			URL string `json:"url"`
		} `json:"links"`
	} `json:"metadata"`
}

func (d seriesDTO) toCore() core.Series {
	s := core.Series{Ref: d.ID, Title: d.Metadata.Title, LibraryRef: d.LibraryID}
	if s.Title == "" {
		s.Title = d.Name
	}
	for _, t := range d.Metadata.AlternateTitles {
		s.AltTitles = append(s.AltTitles, t.Title)
	}
	urls := make([]string, 0, len(d.Metadata.Links))
	for _, l := range d.Metadata.Links {
		urls = append(urls, l.URL)
	}
	s.IDs = match.IDsFromLinks(urls)
	return s
}

func (c *Client) list(ctx context.Context, body any) ([]core.Series, error) {
	var page struct {
		Content []seriesDTO `json:"content"`
	}
	if err := c.api.DoJSON(ctx, http.MethodPost, c.cfg.URL+"/api/v1/series/list?unpaged=true", c.header(), body, &page); err != nil {
		return nil, err
	}
	out := make([]core.Series, 0, len(page.Content))
	for _, d := range page.Content {
		if d.Deleted {
			continue
		}
		out = append(out, d.toCore())
	}
	return out, nil
}

func readStatus(v string) map[string]any {
	return map[string]any{"readStatus": map[string]any{"operator": "is", "value": v}}
}

func (c *Client) ListStartedSeries(ctx context.Context) ([]core.Series, error) {
	return c.list(ctx, map[string]any{"condition": map[string]any{
		"anyOf": []any{readStatus("IN_PROGRESS"), readStatus("READ")},
	}})
}

func (c *Client) ListAllSeries(ctx context.Context) ([]core.Series, error) {
	return c.list(ctx, map[string]any{})
}

func (c *Client) GetSeries(ctx context.Context, ref string) (core.Series, error) {
	var d seriesDTO
	if err := c.api.DoJSON(ctx, http.MethodGet, c.cfg.URL+"/api/v1/series/"+url.PathEscape(ref), c.header(), nil, &d); err != nil {
		return core.Series{}, err
	}
	return d.toCore(), nil
}

func (c *Client) GetProgress(ctx context.Context, ref string) (core.ReadProgress, error) {
	var d struct {
		BooksCount                   int     `json:"booksCount"`
		BooksReadCount               int     `json:"booksReadCount"`
		BooksInProgressCount         int     `json:"booksInProgressCount"`
		LastReadContinuousNumberSort float64 `json:"lastReadContinuousNumberSort"`
		MaxNumberSort                float64 `json:"maxNumberSort"`
	}
	path := c.cfg.URL + "/api/v2/series/" + url.PathEscape(ref) + "/read-progress/tachiyomi"
	if err := c.api.DoJSON(ctx, http.MethodGet, path, c.header(), nil, &d); err != nil {
		return core.ReadProgress{}, err
	}
	unit := core.UnitChapter
	if len(c.cfg.VolumeLibraries) > 0 {
		s, err := c.GetSeries(ctx, ref)
		if err != nil {
			return core.ReadProgress{}, err
		}
		if slices.Contains(c.cfg.VolumeLibraries, s.LibraryRef) {
			unit = core.UnitVolume
		}
	}
	return core.ReadProgress{
		Unit: unit, BooksTotal: d.BooksCount, BooksRead: d.BooksReadCount, BooksInProgress: d.BooksInProgressCount,
		LastReadNumber: d.LastReadContinuousNumberSort, MaxNumber: d.MaxNumberSort,
	}, nil
}
