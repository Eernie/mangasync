// Package suwayomi is the Downloader adapter for Suwayomi-Server (GraphQL API).
package suwayomi

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"mangasync/internal/httpx"
)

type Config struct {
	URL       string
	Auth      string // none | basic | ui_login
	User      string
	Pass      string
	Sources   []string // ordered display names or numeric IDs
	Threshold float64
}

type source struct {
	ID   string
	Name string
}

type Client struct {
	cfg  Config
	http *httpx.Client
	now  func() time.Time

	mu      sync.Mutex
	access  string
	refresh string
	sources []source
	lib     []mangaDTO
	libAt   time.Time
}

func New(cfg Config, hc *httpx.Client) (*Client, error) {
	switch cfg.Auth {
	case "none":
	case "basic", "ui_login":
		if cfg.User == "" || cfg.Pass == "" {
			return nil, fmt.Errorf("suwayomi: auth %q needs a user and password", cfg.Auth)
		}
	default:
		return nil, fmt.Errorf("suwayomi: unknown auth mode %q (none, basic, ui_login)", cfg.Auth)
	}
	cfg.URL = strings.TrimRight(cfg.URL, "/")
	return &Client{cfg: cfg, http: hc, now: time.Now}, nil
}

func (c *Client) Name() string { return "suwayomi" }

// Init logs in (ui_login) and resolves the configured sources. Call once before use.
func (c *Client) Init(ctx context.Context) error {
	if c.cfg.Auth == "ui_login" {
		if err := c.login(ctx); err != nil {
			return fmt.Errorf("suwayomi login: %w", err)
		}
	}
	return c.resolveSources(ctx)
}

func (c *Client) resolveSources(ctx context.Context) error {
	if len(c.cfg.Sources) == 0 {
		return fmt.Errorf("suwayomi: no sources configured")
	}
	var out struct {
		Sources struct {
			Nodes []struct {
				ID          string `json:"id"`
				DisplayName string `json:"displayName"`
			} `json:"nodes"`
		} `json:"sources"`
	}
	if err := c.gql(ctx, `{ sources { nodes { id displayName } } }`, nil, &out); err != nil {
		return fmt.Errorf("list sources: %w", err)
	}
	var resolved []source
	for _, want := range c.cfg.Sources {
		found := false
		for _, n := range out.Sources.Nodes {
			if n.ID == want || strings.EqualFold(n.DisplayName, want) {
				resolved = append(resolved, source{ID: n.ID, Name: n.DisplayName})
				found = true
				break
			}
		}
		if !found {
			names := make([]string, 0, len(out.Sources.Nodes))
			for _, n := range out.Sources.Nodes {
				names = append(names, n.DisplayName)
			}
			return fmt.Errorf("suwayomi source %q is not installed (available: %s)", want, strings.Join(names, ", "))
		}
	}
	c.mu.Lock()
	c.sources = resolved
	c.mu.Unlock()
	return nil
}
