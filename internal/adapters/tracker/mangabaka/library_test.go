package mangabaka

import (
	"io"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"mangasync/internal/core"
)

// v2 series objects: no title/native_title/romanized_title/secondary_titles, only `titles`.
const chainsawV2 = `{"id":1677,"state":"active","merged_with":null,"status":"releasing",
"titles":[
{"language":"ja","traits":["native"],"title":"チェンソーマン","is_primary":false},
{"language":"en","traits":["official"],"title":"Chainsaw Man","is_primary":true},
{"language":"en","traits":["alternative"],"title":"Chain Saw Man","is_primary":false}],
"source":{"anilist":{"id":105778},"manga_updates":{"id":"ylx5wzn"},"kitsu":{"id":null}}}`

const borutoV2 = `{"id":2000,"state":"active","status":"completed",
"titles":[{"language":"en","traits":["official"],"title":"Boruto: Naruto Next Generations","is_primary":true}]}`

func TestListLibraryPagesAndMaps(t *testing.T) {
	c, rec := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if r.URL.Path != "/v2/my/library" || len(q["state"]) != 1 ||
			q.Get("limit") != "100" || q.Get("schema") != "full" {
			t.Errorf("unexpected request %s", r.URL)
		}
		switch q.Get("state") + "/" + q.Get("page") {
		case "plan_to_read/1":
			io.WriteString(w, `{"status":200,"pagination":{"next":null},"data":[
				{"entry":{"series_id":1677,"state":"plan_to_read"},"lists":[],"series":`+chainsawV2+`}]}`)
		case "dropped/1":
			io.WriteString(w, `{"status":200,"pagination":{"next":"page2"},"data":[
				{"entry":{"series_id":2000,"state":"dropped"},"lists":[],"series":`+borutoV2+`}]}`)
		case "dropped/2":
			io.WriteString(w, `{"status":200,"pagination":{"next":null},"data":[]}`)
		default:
			t.Errorf("unexpected state/page %q", q.Get("state")+"/"+q.Get("page"))
		}
	}))

	got, err := c.ListLibrary(t.Context(), []core.Status{core.StatusPlanning, core.StatusDropped})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || len(rec.all()) != 3 {
		t.Fatalf("entries = %+v, requests = %d", got, len(rec.all()))
	}
	var states []string
	for _, r := range rec.all() {
		for _, p := range strings.FieldsFunc(r, func(c rune) bool { return c == '?' || c == '&' || c == ' ' }) {
			if v, ok := strings.CutPrefix(p, "state="); ok {
				states = append(states, v)
			}
		}
	}
	if !slices.Equal(states, []string{"plan_to_read", "dropped", "dropped"}) {
		t.Errorf("states requested = %v", states)
	}
	first := got[0]
	if first.Status != core.StatusPlanning || first.Series.Ref != "1677" || first.Series.Title != "Chainsaw Man" ||
		strings.Join(first.Series.AltTitles, "|") != "Chain Saw Man|チェンソーマン" ||
		first.Series.IDs[core.IDAniList] != "105778" || first.Series.IDs[core.IDMangaUpdates] != "ylx5wzn" ||
		first.Series.IDs[core.IDMangaBaka] != "1677" {
		t.Errorf("first = %+v", first)
	}
	if got[1].Status != core.StatusDropped || got[1].Series.Ref != "2000" || got[1].Series.Title != "Boruto: Naruto Next Generations" {
		t.Errorf("second = %+v", got[1])
	}
}

func TestListLibraryDedupesOverlap(t *testing.T) {
	c, _ := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// a server that ignores the state filter and returns the same series for every request
		io.WriteString(w, `{"status":200,"pagination":{"next":null},"data":[
			{"entry":{"series_id":2000,"state":"dropped"},"lists":[],"series":`+borutoV2+`}]}`)
	}))
	got, err := c.ListLibrary(t.Context(), []core.Status{core.StatusPlanning, core.StatusDropped})
	if err != nil || len(got) != 1 || got[0].Series.Ref != "2000" {
		t.Fatalf("got %+v, err %v", got, err)
	}
}

func TestListLibraryStopsAtCount(t *testing.T) {
	var pages atomic.Int32
	c, _ := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if pages.Add(1) > 3 { // let the loop end even if the client misbehaves
			t.Error("kept paging past count")
			io.WriteString(w, `{"status":200,"pagination":{"next":null},"data":[]}`)
			return
		}
		// a misbehaving server that always claims a next page
		io.WriteString(w, `{"status":200,"pagination":{"next":"more","count":1},"data":[
			{"entry":{"series_id":2000,"state":"dropped"},"lists":[],"series":`+borutoV2+`}]}`)
	}))
	got, err := c.ListLibrary(t.Context(), []core.Status{core.StatusDropped})
	if err != nil || len(got) != 1 || pages.Load() != 1 {
		t.Fatalf("got %d entries, %d pages, err %v", len(got), pages.Load(), err)
	}
}

func TestListLibraryNothingRequested(t *testing.T) {
	c, rec := newTestClient(t, http.NotFoundHandler())
	got, err := c.ListLibrary(t.Context(), []core.Status{core.StatusUnknown})
	if err != nil || got != nil || len(rec.all()) != 0 {
		t.Fatalf("got %v, %v, %d requests", got, err, len(rec.all()))
	}
}
