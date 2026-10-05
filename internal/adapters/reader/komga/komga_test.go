package komga

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"mangasync/internal/core"
	"mangasync/internal/core/coretest"
	"mangasync/internal/httpx"
)

const seriesS1 = `{"id":"S1","libraryId":"L1","name":"Chainsaw Man","metadata":{"title":"Chainsaw Man",
"alternateTitles":[{"label":"Japanese","title":"チェンソーマン"}],
"links":[{"label":"AniList","url":"https://anilist.co/manga/105778"},
{"label":"MangaUpdates","url":"https://www.mangaupdates.com/series/ylx5wzn/chainsaw-man"},
{"label":"Amazon","url":"https://www.amazon.co.jp/dp/B07MX551PW"}]}}`

const seriesS2 = `{"id":"S2","libraryId":"L2","name":"LOSTEND","metadata":{"title":"","alternateTitles":[],"links":[]}}`

const progressS1 = `{"booksCount":244,"booksReadCount":104,"booksUnreadCount":139,"booksInProgressCount":1,
"lastReadContinuousNumberSort":104.0,"maxNumberSort":232.0}`

type server struct {
	*httptest.Server
	mu         sync.Mutex
	listBodies []string
}

func newServer(t *testing.T) *server {
	t.Helper()
	s := &server{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/series/list", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("unpaged") != "true" {
			t.Errorf("list without unpaged=true: %s", r.URL)
		}
		b, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.listBodies = append(s.listBodies, string(b))
		s.mu.Unlock()
		fmt.Fprintf(w, `{"content":[%s,%s]}`, seriesS1, seriesS2)
	})
	mux.HandleFunc("GET /api/v1/series/{id}", func(w http.ResponseWriter, r *http.Request) {
		switch r.PathValue("id") {
		case "S1":
			io.WriteString(w, seriesS1)
		case "S2":
			io.WriteString(w, seriesS2)
		default:
			http.NotFound(w, r)
		}
	})
	mux.HandleFunc("GET /api/v2/series/{id}/read-progress/tachiyomi", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, progressS1)
	})
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-API-Key") != "secret" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(s.Close)
	return s
}

func newClient(url string, volumeLibs ...string) *Client {
	return New(Config{URL: url + "/", APIKey: "secret", VolumeLibraries: volumeLibs}, httpx.New(5*time.Second, 0))
}

func TestContract(t *testing.T) {
	srv := newServer(t)
	coretest.ReaderContract(t, newClient(srv.URL), "S1")
}

func TestListBodies(t *testing.T) {
	srv := newServer(t)
	c := newClient(srv.URL)
	if _, err := c.ListStartedSeries(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ListAllSeries(t.Context()); err != nil {
		t.Fatal(err)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if !strings.Contains(srv.listBodies[0], `"IN_PROGRESS"`) || !strings.Contains(srv.listBodies[0], `"READ"`) {
		t.Errorf("started body = %s", srv.listBodies[0])
	}
	if srv.listBodies[1] != `{}` {
		t.Errorf("all body = %s", srv.listBodies[1])
	}
}

func TestGetSeriesMapsTitlesAndIDs(t *testing.T) {
	c := newClient(newServer(t).URL)
	s, err := c.GetSeries(t.Context(), "S1")
	if err != nil {
		t.Fatal(err)
	}
	if s.Title != "Chainsaw Man" || s.LibraryRef != "L1" || len(s.AltTitles) != 1 || s.AltTitles[0] != "チェンソーマン" {
		t.Errorf("series = %+v", s)
	}
	if s.IDs[core.IDAniList] != "105778" || s.IDs[core.IDMangaUpdates] != "ylx5wzn" || len(s.IDs) != 2 {
		t.Errorf("ids = %v", s.IDs)
	}
	s2, _ := c.GetSeries(t.Context(), "S2")
	if s2.Title != "LOSTEND" {
		t.Errorf("empty metadata title should fall back to name, got %q", s2.Title)
	}
}

func TestGetProgress(t *testing.T) {
	srv := newServer(t)
	p, err := newClient(srv.URL).GetProgress(t.Context(), "S1")
	if err != nil {
		t.Fatal(err)
	}
	want := core.ReadProgress{Unit: core.UnitChapter, BooksTotal: 244, BooksRead: 104, BooksInProgress: 1, LastReadNumber: 104, MaxNumber: 232}
	if p != want {
		t.Errorf("progress = %+v, want %+v", p, want)
	}
	p, _ = newClient(srv.URL, "L1").GetProgress(t.Context(), "S1")
	if p.Unit != core.UnitVolume {
		t.Errorf("volume library: unit = %q", p.Unit)
	}
}

func TestBadKey(t *testing.T) {
	srv := newServer(t)
	c := New(Config{URL: srv.URL, APIKey: "wrong"}, httpx.New(5*time.Second, 0))
	if _, err := c.GetSeries(t.Context(), "S1"); !httpx.IsStatus(err, http.StatusUnauthorized) {
		t.Fatalf("err = %v, want 401", err)
	}
}
