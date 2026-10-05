// Package config loads the core (non-adapter) configuration.
package config

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"mangasync/internal/core"
	"mangasync/internal/envutil"
)

type Config struct {
	Reader            string
	Trackers          []string
	DownloadTracker   string
	Downloader        string
	Acquire           []core.Status
	Release           []core.Status
	DownloadInterval  time.Duration
	ReconcileInterval time.Duration
	SSEDebounce       time.Duration
	MatchThreshold    float64
	DryRun            bool
	DBPath            string
	LogLevel          string
	HTTPAddr          string
}

func Load(l envutil.Lookup) (Config, error) {
	c := Config{
		Reader:          cmp.Or(envutil.Get(l, "READER"), "komga"),
		Trackers:        envutil.List(envutil.GetOr(l, "TRACKERS", "mangabaka")),
		DownloadTracker: envutil.Get(l, "DOWNLOAD_TRACKER"),
		Downloader:      envutil.Get(l, "DOWNLOADER"),
		DBPath:          cmp.Or(envutil.Get(l, "DB_PATH"), "/data/mangasync.db"),
		LogLevel:        cmp.Or(envutil.Get(l, "LOG_LEVEL"), "info"),
		HTTPAddr:        cmp.Or(envutil.Get(l, "HTTP_ADDR"), ":8080"),
	}
	var err error
	if c.Acquire, err = core.ParseStatuses(envutil.GetOr(l, "ACQUIRE_STATUSES", "planning,reading,rereading")); err != nil {
		return c, fmt.Errorf("ACQUIRE_STATUSES: %w", err)
	}
	if c.Release, err = core.ParseStatuses(envutil.GetOr(l, "RELEASE_STATUSES", "dropped")); err != nil {
		return c, fmt.Errorf("RELEASE_STATUSES: %w", err)
	}
	for _, d := range []struct {
		key string
		def string
		dst *time.Duration
	}{
		{"DOWNLOAD_INTERVAL", "15m", &c.DownloadInterval},
		{"RECONCILE_INTERVAL", "1h", &c.ReconcileInterval},
		{"SSE_DEBOUNCE", "10s", &c.SSEDebounce},
	} {
		v, err := time.ParseDuration(cmp.Or(envutil.Get(l, d.key), d.def))
		if err != nil || v <= 0 {
			return c, fmt.Errorf("%s: must be a positive duration like %q", d.key, d.def)
		}
		*d.dst = v
	}
	if c.MatchThreshold, err = strconv.ParseFloat(cmp.Or(envutil.Get(l, "MATCH_THRESHOLD"), "0.9"), 64); err != nil ||
		c.MatchThreshold <= 0 || c.MatchThreshold > 1 {
		return c, errors.New("MATCH_THRESHOLD: must be a number in (0, 1]")
	}
	if c.DryRun, err = strconv.ParseBool(cmp.Or(envutil.Get(l, "DRY_RUN"), "false")); err != nil {
		return c, errors.New("DRY_RUN: must be true or false")
	}

	if len(c.Trackers) == 0 {
		return c, errors.New("TRACKERS: at least one tracker is required")
	}
	if (c.DownloadTracker == "") != (c.Downloader == "") {
		return c, errors.New("DOWNLOAD_TRACKER and DOWNLOADER must be set together (or both empty to disable download sync)")
	}
	for _, s := range c.Acquire {
		if slices.Contains(c.Release, s) {
			return c, fmt.Errorf("status %q is in both ACQUIRE_STATUSES and RELEASE_STATUSES", s)
		}
	}
	return c, nil
}
