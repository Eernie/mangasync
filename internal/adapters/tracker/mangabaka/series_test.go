package mangabaka

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"mangasync/internal/core"
	"mangasync/internal/core/coretest"
)

const chainsaw = `{"id":1677,"state":"active","merged_with":null,"title":"Chainsaw Man","native_title":"チェンソーマン",
"romanized_title":"Chainsaw Man","secondary_titles":{"en":[{"type":"alternative","title":"Chain Saw Man"}],
"ko":[{"type":"official","title":"체인소 맨"}]},"status":"releasing",
"source":{"anilist":{"id":105778},"manga_updates":{"id":"ylx5wzn"},"my_anime_list":{"id":116778},"kitsu":{"id":null}}}`

const boruto = `{"id":2000,"state":"active","title":"Boruto: Naruto Next Generations","status":"completed"}`
const merged = `{"id":10,"state":"merged","merged_with":1677,"title":"Chainsaw Man (dup)"}`

const sasukeSearch = `{"status":200,"data":[
{"id":84642,"state":"active","title":"Naruto Retsuden","status":"completed"},
{"id":56702,"state":"active","title":"Naruto: Sasuke’s Story—The Uchiha and the Heavenly Stardust: The Manga","status":"completed"},
{"id":374014,"state":"active","title":"Sasuke's Story: The Uchiha and the Heavenly Stardust","status":"completed"}]}`

func seriesHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/series/{id}", func(w http.ResponseWriter, r *http.Request) {
		switch r.PathValue("id") {
		case "1677":
			io.WriteString(w, `{"status":200,"data":`+chainsaw+`}`)
		case "2000":
			io.WriteString(w, `{"status":200,"data":`+boruto+`}`)
		case "10":
			io.WriteString(w, `{"status":200,"data":`+merged+`}`)
		default:
			http.NotFound(w, r)
		}
	})
	mux.HandleFunc("GET /v1/source/anilist/{id}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("id") != "105778" {
			http.NotFound(w, r)
			return
		}
		io.WriteString(w, `{"status":200,"data":{"source_response":null,"series":[`+chainsaw+`]}}`)
	})
	mux.HandleFunc("GET /v1/source/manga-updates/{id}", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"status":200,"data":{"series":[`+boruto+`]}}`)
	})
	mux.HandleFunc("GET /v1/series/search", func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Query().Get("q"), "Sasuke") {
			io.WriteString(w, sasukeSearch)
			return
		}
		io.WriteString(w, `{"status":200,"data":[]}`)
	})
	mux.HandleFunc("GET /v1/my/library/{id}", func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) })
	return mux
}

func TestSeriesToCore(t *testing.T) {
	c, _ := newTestClient(t, seriesHandler())
	d, err := c.getSeries(t.Context(), "1677")
	if err != nil {
		t.Fatal(err)
	}
	s := d.toCore()
	wantAlt := []string{"Chain Saw Man", "체인소 맨", "チェンソーマン"}
	if s.Ref != "1677" || s.Title != "Chainsaw Man" || strings.Join(s.AltTitles, "|") != strings.Join(wantAlt, "|") {
		t.Errorf("series = %+v", s)
	}
	if s.IDs[core.IDMangaBaka] != "1677" || s.IDs[core.IDAniList] != "105778" || s.IDs[core.IDMangaUpdates] != "ylx5wzn" ||
		s.IDs[core.IDMAL] != "116778" {
		t.Errorf("ids = %v", s.IDs)
	}
	if _, ok := s.IDs[core.IDKitsu]; ok {
		t.Error("null source id must be skipped")
	}
}

func TestResolve(t *testing.T) {
	c, rec := newTestClient(t, seriesHandler())
	cases := []struct {
		name   string
		s      core.Series
		wantID string
		found  bool
	}{
		{"direct mangabaka id", core.Series{IDs: core.IDs{core.IDMangaBaka: "1677"}}, "1677", true},
		{"merged mangabaka id", core.Series{IDs: core.IDs{core.IDMangaBaka: "10"}}, "1677", true},
		{"anilist lookup", core.Series{Title: "x", IDs: core.IDs{core.IDAniList: "105778"}}, "1677", true},
		{"anilist 404 falls through to mangaupdates", core.Series{IDs: core.IDs{core.IDAniList: "1", core.IDMangaUpdates: "abc"}}, "2000", true},
		{"title search picks best", core.Series{Title: "Naruto_ Sasuke's Story - The Uchiha and the Heavenly Stardust_ The Manga"}, "56702", true},
		{"title search no match", core.Series{Title: "Nothing Like It"}, "", false},
	}
	for _, c2 := range cases {
		id, found, err := c.Resolve(t.Context(), c2.s)
		if err != nil || id != c2.wantID || found != c2.found {
			t.Errorf("%s: got %q, %v, %v; want %q, %v", c2.name, id, found, err, c2.wantID, c2.found)
		}
	}
	for _, r := range rec.all() {
		if strings.Contains(r, "/v1/series/search") && strings.Contains(r, "Chainsaw") {
			t.Errorf("ID-based resolve must not search: %s", r)
		}
	}
}

func TestSeriesEndedIsCached(t *testing.T) {
	c, rec := newTestClient(t, seriesHandler())
	for range 2 {
		ended, err := c.SeriesEnded(t.Context(), "2000")
		if err != nil || !ended {
			t.Fatalf("boruto ended = %v, %v", ended, err)
		}
	}
	if n := len(rec.all()); n != 1 {
		t.Fatalf("requests = %d, want 1 (cached)", n)
	}
	if ended, _ := c.SeriesEnded(t.Context(), "1677"); ended {
		t.Fatal("releasing series must not be ended")
	}
}

func TestContract(t *testing.T) {
	c, _ := newTestClient(t, seriesHandler())
	coretest.TrackerContract(t, c, core.Series{Ref: "K1", Title: "Chainsaw Man", IDs: core.IDs{core.IDAniList: "105778"}}, "1677")
}
