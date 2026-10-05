package mangabaka

import (
	"encoding/json"
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

func TestSeriesToCoreV2Titles(t *testing.T) {
	var d seriesDTO
	if err := json.Unmarshal([]byte(chainsawV2), &d); err != nil {
		t.Fatal(err)
	}
	s := d.toCore()
	if s.Title != "Chainsaw Man" || strings.Join(s.AltTitles, "|") != "Chain Saw Man|チェンソーマン" {
		t.Errorf("primary: %+v", s)
	}
	// no is_primary: first English title wins; Latin-script alts come before other scripts.
	d = seriesDTO{ID: 5, Titles: []v2TitleDTO{
		{Language: "ja", Title: "ナルト"}, {Language: "fr", Title: "Naruto FR"}, {Language: "en", Title: "Naruto"},
		{Language: "ko", Title: "나루토"}, {Language: "en", Title: "Naruto"}}}
	s = d.toCore()
	if s.Title != "Naruto" || strings.Join(s.AltTitles, "|") != "Naruto FR|ナルト|나루토" {
		t.Errorf("fallback: %+v", s)
	}
	// no English at all: first entry.
	d = seriesDTO{ID: 6, Titles: []v2TitleDTO{{Language: "ja", Title: "ナルト"}, {Language: "ko", Title: "나루토"}}}
	if s = d.toCore(); s.Title != "ナルト" || len(s.AltTitles) != 1 || s.AltTitles[0] != "나루토" {
		t.Errorf("no english: %+v", s)
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

func TestResolveSourceLookupStates(t *testing.T) {
	const deleted = `{"id":11,"state":"deleted","title":"Gone"}`
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/series/{id}", func(w http.ResponseWriter, r *http.Request) {
		switch r.PathValue("id") {
		case "1677":
			io.WriteString(w, `{"status":200,"data":`+chainsaw+`}`)
		case "2000":
			io.WriteString(w, `{"status":200,"data":`+boruto+`}`)
		case "10":
			io.WriteString(w, `{"status":200,"data":`+merged+`}`)
		case "11":
			io.WriteString(w, `{"status":200,"data":`+deleted+`}`)
		default:
			http.NotFound(w, r)
		}
	})
	mux.HandleFunc("GET /v1/source/anilist/{id}", func(w http.ResponseWriter, r *http.Request) {
		switch r.PathValue("id") {
		case "del-active":
			io.WriteString(w, `{"status":200,"data":{"series":[`+deleted+`,`+boruto+`]}}`)
		case "merged":
			io.WriteString(w, `{"status":200,"data":{"series":[`+merged+`]}}`)
		case "deleted-only":
			io.WriteString(w, `{"status":200,"data":{"series":[`+deleted+`]}}`)
		case "bad":
			http.Error(w, "bad id", http.StatusBadRequest)
		default:
			http.NotFound(w, r)
		}
	})
	mux.HandleFunc("GET /v1/source/manga-updates/{id}", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"status":200,"data":{"series":[`+chainsaw+`]}}`)
	})
	mux.HandleFunc("GET /v1/series/search", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"status":200,"data":[]}`)
	})
	c, _ := newTestClient(t, mux)

	cases := []struct {
		name   string
		ids    core.IDs
		wantID string
		found  bool
	}{
		{"deleted then active picks active", core.IDs{core.IDAniList: "del-active"}, "2000", true},
		{"merged follows merged_with", core.IDs{core.IDAniList: "merged"}, "1677", true},
		{"deleted only falls through to next source", core.IDs{core.IDAniList: "deleted-only", core.IDMangaUpdates: "x"}, "1677", true},
		{"deleted only, nothing else, not found", core.IDs{core.IDAniList: "deleted-only"}, "", false},
		{"400 is treated like 404", core.IDs{core.IDAniList: "bad", core.IDMangaUpdates: "x"}, "1677", true},
	}
	for _, tc := range cases {
		id, found, err := c.Resolve(t.Context(), core.Series{IDs: tc.ids})
		if err != nil || id != tc.wantID || found != tc.found {
			t.Errorf("%s: got %q, %v, %v; want %q, %v", tc.name, id, found, err, tc.wantID, tc.found)
		}
	}
}

func TestFollowMergedDeletedAndLoops(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/series/{id}", func(w http.ResponseWriter, r *http.Request) {
		switch r.PathValue("id") {
		case "11": // deleted
			io.WriteString(w, `{"status":200,"data":{"id":11,"state":"deleted"}}`)
		case "20": // merge cycle 20 -> 21 -> 20
			io.WriteString(w, `{"status":200,"data":{"id":20,"state":"merged","merged_with":21}}`)
		case "21":
			io.WriteString(w, `{"status":200,"data":{"id":21,"state":"merged","merged_with":20}}`)
		default:
			http.NotFound(w, r)
		}
	})
	c, _ := newTestClient(t, mux)
	for _, id := range []string{"11", "20"} {
		got, found, err := c.Resolve(t.Context(), core.Series{IDs: core.IDs{core.IDMangaBaka: id}})
		if err != nil || found || got != "" {
			t.Errorf("Resolve(mangabaka=%s) = %q, %v, %v; want not found", id, got, found, err)
		}
	}
}

func TestResolveSearchSkipsDeletedAndFollowsMerged(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/series/{id}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("id") == "1677" {
			io.WriteString(w, `{"status":200,"data":`+chainsaw+`}`)
			return
		}
		http.NotFound(w, r)
	})
	mux.HandleFunc("GET /v1/series/search", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("q") {
		case "Dead Exact":
			io.WriteString(w, `{"status":200,"data":[
				{"id":11,"state":"deleted","title":"Dead Exact"},
				{"id":12,"state":"active","title":"Dead Exact"}]}`)
		case "Merged Exact":
			io.WriteString(w, `{"status":200,"data":[{"id":10,"state":"merged","merged_with":1677,"title":"Merged Exact"}]}`)
		case "Merged Broken":
			io.WriteString(w, `{"status":200,"data":[{"id":13,"state":"merged","merged_with":999,"title":"Merged Broken"}]}`)
		case "Only Deleted":
			io.WriteString(w, `{"status":200,"data":[{"id":11,"state":"deleted","title":"Only Deleted"}]}`)
		default:
			io.WriteString(w, `{"status":200,"data":[]}`)
		}
	})
	c, _ := newTestClient(t, mux)
	cases := []struct {
		title  string
		wantID string
		found  bool
	}{
		{"Dead Exact", "12", true},
		{"Merged Exact", "1677", true},
		{"Merged Broken", "", false},
		{"Only Deleted", "", false},
	}
	for _, tc := range cases {
		id, found, err := c.Resolve(t.Context(), core.Series{Title: tc.title})
		if err != nil || id != tc.wantID || found != tc.found {
			t.Errorf("%s: got %q, %v, %v; want %q, %v", tc.title, id, found, err, tc.wantID, tc.found)
		}
	}
}
