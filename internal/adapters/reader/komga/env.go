package komga

import (
	"errors"
	"time"

	"mangasync/internal/envutil"
	"mangasync/internal/httpx"
)

// FromEnv reads KOMGA_URL, KOMGA_API_KEY and KOMGA_VOLUME_LIBRARIES.
func FromEnv(l envutil.Lookup) (*Client, error) {
	cfg := Config{
		URL:             envutil.Get(l, "KOMGA_URL"),
		APIKey:          envutil.Get(l, "KOMGA_API_KEY"),
		VolumeLibraries: envutil.List(envutil.Get(l, "KOMGA_VOLUME_LIBRARIES")),
	}
	if cfg.URL == "" || cfg.APIKey == "" {
		return nil, errors.New("komga: KOMGA_URL and KOMGA_API_KEY are required")
	}
	return New(cfg, httpx.New(30*time.Second, 0)), nil
}
