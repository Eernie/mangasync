// Package mangabaka is the Tracker adapter for MangaBaka (https://api.mangabaka.org).
package mangabaka

import (
	"net/http"
	"strings"
	"sync"
	"time"

	"mangasync/internal/httpx"
)

const DefaultBaseURL = "https://api.mangabaka.org"

type Config struct {
	Token     string // Personal Access Token (mb-...)
	BaseURL   string // default DefaultBaseURL
	Threshold float64
	EndedTTL  time.Duration // cache lifetime for SeriesEnded
}

type Client struct {
	cfg    Config
	api    *httpx.Client // general + /my/* calls
	search *httpx.Client // /v1/series/search (stricter rate limit)
	now    func() time.Time

	mu    sync.Mutex
	ended map[string]endedEntry
}

type endedEntry struct {
	ended bool
	at    time.Time
}

func New(cfg Config, api, search *httpx.Client) *Client {
	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultBaseURL
	}
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	if cfg.EndedTTL <= 0 {
		cfg.EndedTTL = time.Hour
	}
	return &Client{cfg: cfg, api: api, search: search, now: time.Now, ended: map[string]endedEntry{}}
}

func (c *Client) Name() string { return "mangabaka" }

func (c *Client) header() http.Header {
	h := http.Header{}
	h.Set("x-api-key", c.cfg.Token)
	return h
}

func (c *Client) url(path string) string { return c.cfg.BaseURL + path }
