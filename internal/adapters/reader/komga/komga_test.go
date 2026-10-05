package komga

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
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

const seriesS3 = `{"id":"S3","libraryId":"L1","name":"Gone","deleted":true,"metadata":{"title":"Gone","alternateTitles":[],"links":[]}}`

const progressS1 = `{"booksCount":244,"booksReadCount":104,"booksUnreadCount":139,"booksInProgressCount":1,
"lastReadContinuousNumberSort":104.0,"maxNumberSort":232.0}`

const progressUnread = `{"booksCount":10,"booksReadCount":0,"booksUnreadCount":10,"booksInProgressCount":0,
"lastReadContinuousNumberSort":0.0,"maxNumberSort":10.0}`

type server struct {
	*httptest.Server
	mu             sync.Mutex
	listBodies     []string
	getHits        int
	bookListStatus int      // non-zero: books/list answers with this status
	bookLists      []string // "<sort>|<body>" of every POST /api/v1/books/list
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
		fmt.Fprintf(w, `{"content":[%s,%s,%s]}`, seriesS1, seriesS2, seriesS3)
	})
	mux.HandleFunc("GET /api/v1/series/{id}", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.getHits++
		s.mu.Unlock()
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
		if r.PathValue("id") == "S4" {
			io.WriteString(w, progressUnread)
			return
		}
		io.WriteString(w, progressS1)
	})
	mux.HandleFunc("POST /api/v1/books/list", func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		sort := r.URL.Query().Get("sort")
		s.mu.Lock()
		s.bookLists = append(s.bookLists, sort+"|"+string(b))
		status := s.bookListStatus
		s.mu.Unlock()
		if status != 0 {
			http.Error(w, "boom", status)
			return
		}
		if r.URL.Query().Get("size") != "1" {
			t.Errorf("books/list size = %q, want 1", r.URL.Query().Get("size"))
		}
		var body struct {
			Condition struct {
				AllOf []map[string]any `json:"allOf"`
			} `json:"condition"`
		}
		if err := json.Unmarshal(b, &body); err != nil || len(body.Condition.AllOf) != 2 {
			t.Errorf("books/list body = %s (%v)", b, err)
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		if sid, _ := body.Condition.AllOf[0]["seriesId"].(map[string]any); sid["operator"] != "is" || sid["value"] != "S1" {
			t.Errorf("books/list seriesId condition = %v, want is S1", body.Condition.AllOf[0])
		}
		anyOf, _ := body.Condition.AllOf[1]["anyOf"].([]any)
		var statuses []string
		for _, c := range anyOf {
			rs, _ := c.(map[string]any)["readStatus"].(map[string]any)
			statuses = append(statuses, fmt.Sprint(rs["operator"], ":", rs["value"]))
		}
		if !slices.Equal(statuses, []string{"is:READ", "is:IN_PROGRESS"}) {
			t.Errorf("books/list readStatus conditions = %v", statuses)
		}
		switch sort {
		case "readProgress.readDate,asc":
			io.WriteString(w, `{"content":[{"id":"B1","readProgress":{"page":10,"completed":true,"readDate":"2026-06-06T13:13:42Z"}}]}`)
		case "readProgress.readDate,desc":
			io.WriteString(w, `{"content":[{"id":"B9","readProgress":{"page":3,"completed":false,"readDate":"2026-09-01T08:00:00.123Z"}}]}`)
		default:
			t.Errorf("books/list sort = %q", sort)
			http.Error(w, "bad sort", http.StatusBadRequest)
		}
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

func TestListAllSeriesBody(t *testing.T) {
	srv := newServer(t)
	if _, err := newClient(srv.URL).ListAllSeries(t.Context()); err != nil {
		t.Fatal(err)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.listBodies) != 1 || srv.listBodies[0] != `{}` {
		t.Errorf("list bodies = %q, want one `{}`", srv.listBodies)
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
	want := core.ReadProgress{Unit: core.UnitChapter, BooksTotal: 244, BooksRead: 104, BooksInProgress: 1, LastReadNumber: 104, MaxNumber: 232,
		FirstReadAt: time.Date(2026, 6, 6, 13, 13, 42, 0, time.UTC), LastReadAt: time.Date(2026, 9, 1, 8, 0, 0, 123_000_000, time.UTC)}
	if !p.FirstReadAt.Equal(want.FirstReadAt) || !p.LastReadAt.Equal(want.LastReadAt) {
		t.Errorf("read dates = %v / %v, want %v / %v", p.FirstReadAt, p.LastReadAt, want.FirstReadAt, want.LastReadAt)
	}
	p.FirstReadAt, p.LastReadAt, want.FirstReadAt, want.LastReadAt = time.Time{}, time.Time{}, time.Time{}, time.Time{}
	if p != want {
		t.Errorf("progress = %+v, want %+v", p, want)
	}
	srv.mu.Lock()
	lists := slices.Clone(srv.bookLists)
	srv.mu.Unlock()
	if len(lists) != 2 || !strings.HasPrefix(lists[0], "readProgress.readDate,asc|") || !strings.HasPrefix(lists[1], "readProgress.readDate,desc|") {
		t.Errorf("books/list calls = %v, want one asc then one desc", lists)
	}
	srv.mu.Lock()
	hits := srv.getHits
	srv.mu.Unlock()
	if hits != 0 {
		t.Errorf("GetSeries hits without volume libraries = %d, want 0", hits)
	}
	p, _ = newClient(srv.URL, "L1").GetProgress(t.Context(), "S1")
	srv.mu.Lock()
	hits = srv.getHits
	srv.mu.Unlock()
	if hits != 1 {
		t.Errorf("GetSeries hits with volume libraries = %d, want 1", hits)
	}
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

func TestListSkipsDeleted(t *testing.T) {
	c := newClient(newServer(t).URL)
	got, err := c.ListAllSeries(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Errorf("got %d series, want 2 (S3 is deleted)", len(got))
	}
	for _, s := range got {
		if s.Ref == "S3" {
			t.Error("deleted series returned")
		}
	}
}

func TestGetProgressWithoutReadBooksSkipsDateLookups(t *testing.T) {
	srv := newServer(t)
	p, err := newClient(srv.URL).GetProgress(t.Context(), "S4")
	if err != nil {
		t.Fatal(err)
	}
	if p.BooksRead != 0 || p.BooksInProgress != 0 || !p.FirstReadAt.IsZero() || !p.LastReadAt.IsZero() {
		t.Errorf("progress = %+v", p)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.bookLists) != 0 {
		t.Errorf("books/list calls = %v, want none", srv.bookLists)
	}
}

func TestGetProgressSurvivesDateLookupFailure(t *testing.T) {
	srv := newServer(t)
	srv.mu.Lock()
	srv.bookListStatus = http.StatusInternalServerError
	srv.mu.Unlock()
	api := httpx.New(5*time.Second, 0)
	api.BaseDelay, api.MaxAttempts = time.Millisecond, 2
	c := New(Config{URL: srv.URL, APIKey: "secret"}, api)
	p, err := c.GetProgress(t.Context(), "S1")
	if err != nil {
		t.Fatalf("GetProgress failed on date lookup error: %v", err)
	}
	if p.BooksTotal != 244 || p.BooksRead != 104 || p.BooksInProgress != 1 || p.LastReadNumber != 104 || p.MaxNumber != 232 {
		t.Errorf("progress = %+v", p)
	}
	if !p.FirstReadAt.IsZero() || !p.LastReadAt.IsZero() {
		t.Errorf("dates = %v / %v, want zero", p.FirstReadAt, p.LastReadAt)
	}
}
