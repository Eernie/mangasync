package suwayomi

import (
	"encoding/base64"
	"net/http"
	"strings"
	"testing"
	"time"

	"mangasync/internal/httpx"
)

func TestResolvesSourcesByNameAndID(t *testing.T) {
	f := newFakeServer(t)
	c := newTestClient(t, f, Config{Sources: []string{"manhuatop (en)", "2131019126180322627"}})
	if len(c.sources) != 2 || c.sources[0].ID != "1903782575226230108" || c.sources[1].Name != "Weeb Central (EN)" {
		t.Fatalf("sources = %+v", c.sources)
	}
}

func TestUnknownSourceFailsInit(t *testing.T) {
	f := newFakeServer(t)
	c, err := New(Config{URL: f.URL, Auth: "none", Sources: []string{"MangaDex (EN)"}, Threshold: 0.9}, httpx.New(5*time.Second, 0))
	if err != nil {
		t.Fatal(err)
	}
	err = c.Init(t.Context())
	if err == nil || !strings.Contains(err.Error(), "MangaDex (EN)") || !strings.Contains(err.Error(), "Weeb Central (EN)") {
		t.Fatalf("err = %v; want it to name the missing and available sources", err)
	}
}

func TestBasicAuth(t *testing.T) {
	f := newFakeServer(t)
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("u:p"))
	f.authCheck = func(r *http.Request) bool { return r.Header.Get("Authorization") == want }
	newTestClient(t, f, Config{Auth: "basic", User: "u", Pass: "p"}) // Init fails if auth is wrong
}

func TestUILoginRefreshesOn401(t *testing.T) {
	f := newFakeServer(t)
	f.authCheck = func(r *http.Request) bool { return r.Header.Get("Authorization") == "Bearer fresh" }
	c := newTestClient(t, f, Config{Auth: "ui_login", User: "u", Pass: "p"})

	c.mu.Lock()
	c.access = "expired"
	c.mu.Unlock()
	if _, err := c.library(t.Context()); err != nil {
		t.Fatalf("expected transparent refresh, got %v", err)
	}
	if len(f.callsMatching("refreshToken(")) != 1 {
		t.Fatalf("refresh calls = %d, want 1", len(f.callsMatching("refreshToken(")))
	}
}

func TestInvalidAuthMode(t *testing.T) {
	if _, err := New(Config{URL: "http://x", Auth: "oauth"}, httpx.New(time.Second, 0)); err == nil {
		t.Fatal("expected error")
	}
	if _, err := New(Config{URL: "http://x", Auth: "basic"}, httpx.New(time.Second, 0)); err == nil {
		t.Fatal("expected error for missing credentials")
	}
}
