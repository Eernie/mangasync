package suwayomi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"mangasync/internal/httpx"
)

type gqlReq struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables"`
}

// fakeServer answers the GraphQL operations this adapter uses. Fields are set by tests.
type fakeServer struct {
	*httptest.Server
	mu        sync.Mutex
	calls     []gqlReq
	authCheck func(r *http.Request) bool   // nil = accept everything
	search    map[string]map[string]string // source ID -> query -> mangas JSON array
	library   string                       // nodes JSON array
	chapters  string                       // chapters JSON array
	errorFor  map[string]string            // source ID -> GraphQL error message
}

func newFakeServer(t *testing.T) *fakeServer {
	t.Helper()
	f := &fakeServer{search: map[string]map[string]string{}, library: `[]`, chapters: `[]`, errorFor: map[string]string{}}
	f.Server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.Close)
	return f
}

func (f *fakeServer) serve(w http.ResponseWriter, r *http.Request) {
	var req gqlReq
	_ = json.NewDecoder(r.Body).Decode(&req)
	f.mu.Lock()
	f.calls = append(f.calls, req)
	f.mu.Unlock()
	q := req.Query
	if strings.Contains(q, "login(") {
		fmt.Fprint(w, `{"data":{"login":{"accessToken":"fresh","refreshToken":"r1"}}}`)
		return
	}
	if strings.Contains(q, "refreshToken(") {
		fmt.Fprint(w, `{"data":{"refreshToken":{"accessToken":"fresh"}}}`)
		return
	}
	if f.authCheck != nil && !f.authCheck(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	switch {
	case strings.Contains(q, "sources"):
		fmt.Fprint(w, `{"data":{"sources":{"nodes":[{"id":"0","displayName":"Local source"},
			{"id":"2131019126180322627","displayName":"Weeb Central (EN)"},{"id":"1903782575226230108","displayName":"ManhuaTop (EN)"}]}}}`)
	case strings.Contains(q, "fetchSourceManga"):
		src, _ := req.Variables["source"].(string)
		query, _ := req.Variables["q"].(string)
		if msg, ok := f.errorFor[src]; ok {
			fmt.Fprintf(w, `{"data":null,"errors":[{"message":%q}]}`, msg)
			return
		}
		mangas := f.search[src][query]
		if mangas == "" {
			mangas = `[]`
		}
		fmt.Fprintf(w, `{"data":{"fetchSourceManga":{"mangas":%s}}}`, mangas)
	case strings.Contains(q, "mangas(condition"):
		fmt.Fprintf(w, `{"data":{"mangas":{"nodes":%s}}}`, f.library)
	case strings.Contains(q, "updateManga"):
		fmt.Fprint(w, `{"data":{"updateManga":{"manga":{"id":1}}}}`)
	case strings.Contains(q, "fetchMangaAndChapters"):
		fmt.Fprintf(w, `{"data":{"fetchMangaAndChapters":{"chapters":%s}}}`, f.chapters)
	case strings.Contains(q, "enqueueChapterDownloads"):
		fmt.Fprint(w, `{"data":{"enqueueChapterDownloads":{"clientMutationId":null}}}`)
	default:
		fmt.Fprintf(w, `{"data":null,"errors":[{"message":"unexpected query: %s"}]}`, strings.ReplaceAll(q, `"`, `'`))
	}
}

func (f *fakeServer) callsMatching(substr string) []gqlReq {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []gqlReq
	for _, c := range f.calls {
		if strings.Contains(c.Query, substr) {
			out = append(out, c)
		}
	}
	return out
}

func newTestClient(t *testing.T, f *fakeServer, cfg Config) *Client {
	t.Helper()
	cfg.URL = f.URL
	if cfg.Auth == "" {
		cfg.Auth = "none"
	}
	if cfg.Threshold == 0 {
		cfg.Threshold = 0.9
	}
	if cfg.Sources == nil {
		cfg.Sources = []string{"Weeb Central (EN)", "ManhuaTop (EN)"}
	}
	hc := httpx.New(5*time.Second, 0)
	hc.BaseDelay = time.Millisecond
	c, err := New(cfg, hc)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Init(t.Context()); err != nil {
		t.Fatal(err)
	}
	return c
}
