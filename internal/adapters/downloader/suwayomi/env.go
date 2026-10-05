package suwayomi

import (
	"cmp"
	"errors"
	"time"

	"mangasync/internal/envutil"
	"mangasync/internal/httpx"
)

// FromEnv reads SUWAYOMI_URL, SUWAYOMI_AUTH, SUWAYOMI_USER, SUWAYOMI_PASS and SUWAYOMI_SOURCES.
// Call Init on the result before use.
func FromEnv(l envutil.Lookup, threshold float64) (*Client, error) {
	cfg := Config{
		URL:       envutil.Get(l, "SUWAYOMI_URL"),
		Auth:      cmp.Or(envutil.Get(l, "SUWAYOMI_AUTH"), "none"),
		User:      envutil.Get(l, "SUWAYOMI_USER"),
		Pass:      envutil.Get(l, "SUWAYOMI_PASS"),
		Sources:   envutil.List(envutil.Get(l, "SUWAYOMI_SOURCES")),
		Threshold: threshold,
	}
	if cfg.URL == "" || len(cfg.Sources) == 0 {
		return nil, errors.New("suwayomi: SUWAYOMI_URL and SUWAYOMI_SOURCES are required")
	}
	// Source searches go through the source website and can be slow. They hit scraped third-party
	// sites (Find makes up to 3 queries x N sources), so throttle to 30 requests/minute.
	hc := httpx.New(2*time.Minute, 30)
	// A stuck search would otherwise take 2 min x 5 attempts; cap the retries.
	hc.MaxAttempts = 2
	return New(cfg, hc)
}
