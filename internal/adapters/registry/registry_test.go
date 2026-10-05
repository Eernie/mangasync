package registry

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mangasync/internal/envutil"
)

func TestUnknownNames(t *testing.T) {
	env := envutil.MapLookup(nil)
	if _, err := Reader("kavita", env); err == nil || !strings.Contains(err.Error(), "komga") {
		t.Errorf("reader err = %v", err)
	}
	if _, err := Tracker("anilist", env, 0.9, time.Hour); err == nil || !strings.Contains(err.Error(), "mangabaka") {
		t.Errorf("tracker err = %v", err)
	}
	if _, err := Downloader(t.Context(), "kaizoku", env, 0.9); err == nil || !strings.Contains(err.Error(), "suwayomi") {
		t.Errorf("downloader err = %v", err)
	}
}

func TestMissingEnv(t *testing.T) {
	env := envutil.MapLookup(nil)
	if _, err := Reader("komga", env); err == nil || !strings.Contains(err.Error(), "KOMGA_URL") {
		t.Errorf("komga err = %v", err)
	}
	if _, err := Tracker("mangabaka", env, 0.9, time.Hour); err == nil || !strings.Contains(err.Error(), "MANGABAKA_TOKEN") {
		t.Errorf("mangabaka err = %v", err)
	}
	if _, err := Downloader(t.Context(), "suwayomi", env, 0.9); err == nil || !strings.Contains(err.Error(), "SUWAYOMI_URL") {
		t.Errorf("suwayomi err = %v", err)
	}
}

func TestBuildsAll(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"data":{"sources":{"nodes":[{"id":"1","displayName":"Weeb Central (EN)"}]}}}`)
	}))
	defer srv.Close()
	env := envutil.MapLookup(map[string]string{
		"KOMGA_URL": "http://komga", "KOMGA_API_KEY": "k", "KOMGA_VOLUME_LIBRARIES": "L1",
		"MANGABAKA_TOKEN": "mb-x",
		"SUWAYOMI_URL":    srv.URL, "SUWAYOMI_SOURCES": "Weeb Central (EN)",
	})
	if r, err := Reader("komga", env); err != nil || r.Name() != "komga" {
		t.Errorf("reader = %v, %v", r, err)
	}
	if tr, err := Tracker("mangabaka", env, 0.9, time.Hour); err != nil || tr.Name() != "mangabaka" {
		t.Errorf("tracker = %v, %v", tr, err)
	}
	if d, err := Downloader(t.Context(), "suwayomi", env, 0.9); err != nil || d.Name() != "suwayomi" {
		t.Errorf("downloader = %v, %v", d, err)
	}
}
