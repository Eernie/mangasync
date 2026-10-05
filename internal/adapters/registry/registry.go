// Package registry maps adapter names to constructors. Adding an adapter means adding
// one case here.
package registry

import (
	"context"
	"fmt"
	"time"

	"mangasync/internal/adapters/downloader/suwayomi"
	"mangasync/internal/adapters/reader/komga"
	"mangasync/internal/adapters/tracker/mangabaka"
	"mangasync/internal/core"
	"mangasync/internal/envutil"
)

func Reader(name string, l envutil.Lookup) (core.Reader, error) {
	switch name {
	case "komga":
		c, err := komga.FromEnv(l)
		if err != nil {
			return nil, err
		}
		return c, nil
	}
	return nil, fmt.Errorf("unknown reader %q (available: komga)", name)
}

func Tracker(name string, l envutil.Lookup, threshold float64, endedTTL time.Duration) (core.Tracker, error) {
	switch name {
	case "mangabaka":
		c, err := mangabaka.FromEnv(l, threshold, endedTTL)
		if err != nil {
			return nil, err
		}
		return c, nil
	}
	return nil, fmt.Errorf("unknown tracker %q (available: mangabaka)", name)
}

// Downloader builds and initialises the named downloader (this contacts the service).
func Downloader(ctx context.Context, name string, l envutil.Lookup, threshold float64) (core.Downloader, error) {
	switch name {
	case "suwayomi":
		c, err := suwayomi.FromEnv(l, threshold)
		if err != nil {
			return nil, err
		}
		if err := c.Init(ctx); err != nil {
			return nil, fmt.Errorf("suwayomi: %w", err)
		}
		return c, nil
	}
	return nil, fmt.Errorf("unknown downloader %q (available: suwayomi)", name)
}
