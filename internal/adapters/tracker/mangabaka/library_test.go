package mangabaka

import (
	"io"
	"net/http"
	"slices"
	"testing"

	"mangasync/internal/core"
)

func TestListLibraryPagesAndMaps(t *testing.T) {
	c, rec := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if r.URL.Path != "/v2/my/library" || !slices.Equal(q["state"], []string{"plan_to_read", "dropped"}) || q.Get("limit") != "100" {
			t.Errorf("unexpected request %s", r.URL)
		}
		switch q.Get("page") {
		case "1":
			io.WriteString(w, `{"status":200,"pagination":{"next":"page2"},"data":[
				{"entry":{"series_id":1677,"state":"plan_to_read"},"lists":[],"series":`+chainsaw+`}]}`)
		case "2":
			io.WriteString(w, `{"status":200,"pagination":{"next":null},"data":[
				{"entry":{"series_id":2000,"state":"dropped"},"lists":[],"series":`+boruto+`}]}`)
		default:
			t.Errorf("unexpected page %q", q.Get("page"))
		}
	}))

	got, err := c.ListLibrary(t.Context(), []core.Status{core.StatusPlanning, core.StatusDropped})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || len(rec.all()) != 2 {
		t.Fatalf("entries = %+v, requests = %d", got, len(rec.all()))
	}
	if got[0].Status != core.StatusPlanning || got[0].Series.Ref != "1677" || got[0].Series.IDs[core.IDAniList] != "105778" ||
		len(got[0].Series.AltTitles) == 0 {
		t.Errorf("first = %+v", got[0])
	}
	if got[1].Status != core.StatusDropped || got[1].Series.Ref != "2000" {
		t.Errorf("second = %+v", got[1])
	}
}

func TestListLibraryNothingRequested(t *testing.T) {
	c, rec := newTestClient(t, http.NotFoundHandler())
	got, err := c.ListLibrary(t.Context(), []core.Status{core.StatusUnknown})
	if err != nil || got != nil || len(rec.all()) != 0 {
		t.Fatalf("got %v, %v, %d requests", got, err, len(rec.all()))
	}
}
