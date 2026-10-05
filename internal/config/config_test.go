package config

import (
	"slices"
	"testing"
	"time"

	"mangasync/internal/core"
	"mangasync/internal/envutil"
)

func TestDefaults(t *testing.T) {
	c, err := Load(envutil.MapLookup(nil))
	if err != nil {
		t.Fatal(err)
	}
	if c.Reader != "komga" || !slices.Equal(c.Trackers, []string{"mangabaka"}) || c.DownloadTracker != "" || c.Downloader != "" {
		t.Errorf("adapters = %+v", c)
	}
	if !slices.Equal(c.Acquire, []core.Status{core.StatusPlanning, core.StatusReading, core.StatusRereading}) ||
		!slices.Equal(c.Release, []core.Status{core.StatusDropped}) {
		t.Errorf("statuses = %v / %v", c.Acquire, c.Release)
	}
	if c.DownloadInterval != 15*time.Minute || c.ReconcileInterval != time.Hour || c.SSEDebounce != 10*time.Second {
		t.Errorf("intervals = %v %v %v", c.DownloadInterval, c.ReconcileInterval, c.SSEDebounce)
	}
	if c.MatchThreshold != 0.9 || c.DryRun || c.DBPath != "/data/mangasync.db" || c.LogLevel != "info" || c.HTTPAddr != ":8080" {
		t.Errorf("misc = %+v", c)
	}
}

func TestCustom(t *testing.T) {
	c, err := Load(envutil.MapLookup(map[string]string{
		"TRACKERS": "mangabaka, anilist", "DOWNLOAD_TRACKER": "mangabaka", "DOWNLOADER": "suwayomi",
		"ACQUIRE_STATUSES": "", "RELEASE_STATUSES": "dropped,paused", "DOWNLOAD_INTERVAL": "5m",
		"MATCH_THRESHOLD": "0.85", "DRY_RUN": "true",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(c.Trackers, []string{"mangabaka", "anilist"}) || c.Acquire != nil || len(c.Release) != 2 ||
		c.DownloadInterval != 5*time.Minute || c.MatchThreshold != 0.85 || !c.DryRun {
		t.Errorf("config = %+v", c)
	}
}

func TestInvalid(t *testing.T) {
	cases := map[string]map[string]string{
		"downloader without tracker": {"DOWNLOADER": "suwayomi"},
		"tracker without downloader": {"DOWNLOAD_TRACKER": "mangabaka"},
		"status in both sets":        {"ACQUIRE_STATUSES": "planning,dropped"},
		"bad status":                 {"RELEASE_STATUSES": "gone"},
		"bad duration":               {"RECONCILE_INTERVAL": "soon"},
		"zero duration":              {"DOWNLOAD_INTERVAL": "0s"},
		"bad threshold":              {"MATCH_THRESHOLD": "1.5"},
		"bad bool":                   {"DRY_RUN": "maybe"},
		"no trackers":                {"TRACKERS": " , "},
	}
	for name, env := range cases {
		if _, err := Load(envutil.MapLookup(env)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
