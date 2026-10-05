package mangabaka

import (
	"errors"
	"time"

	"mangasync/internal/envutil"
	"mangasync/internal/httpx"
)

// FromEnv reads MANGABAKA_TOKEN and optional MANGABAKA_URL. Rate limits stay below the
// API's per-IP limits (search 30/min, other 180/min).
func FromEnv(l envutil.Lookup, threshold float64, endedTTL time.Duration) (*Client, error) {
	cfg := Config{
		Token:     envutil.Get(l, "MANGABAKA_TOKEN"),
		BaseURL:   envutil.Get(l, "MANGABAKA_URL"),
		Threshold: threshold,
		EndedTTL:  endedTTL,
	}
	if cfg.Token == "" {
		return nil, errors.New("mangabaka: MANGABAKA_TOKEN is required")
	}
	return New(cfg, httpx.New(30*time.Second, 150), httpx.New(30*time.Second, 25)), nil
}
