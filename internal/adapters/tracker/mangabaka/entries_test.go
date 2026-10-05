package mangabaka

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"mangasync/internal/core"
	"mangasync/internal/httpx"
)

// recorder captures requests made to a test server.
type recorder struct {
	mu   sync.Mutex
	reqs []string // "METHOD path?query body"
}

func (r *recorder) add(req *http.Request) {
	b, _ := io.ReadAll(req.Body)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reqs = append(r.reqs, req.Method+" "+req.URL.RequestURI()+" "+string(b))
}

func (r *recorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.reqs...)
}

func newTestClient(t *testing.T, h http.Handler) (*Client, *recorder) {
	t.Helper()
	rec := &recorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") != "mb-test" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		rec.add(r)
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	fast := func() *httpx.Client { c := httpx.New(5*time.Second, 0); c.BaseDelay = time.Millisecond; return c }
	return New(Config{Token: "mb-test", BaseURL: srv.URL, Threshold: 0.9, EndedTTL: time.Hour}, fast(), fast()), rec
}

func TestStatusMapping(t *testing.T) {
	cases := map[string]core.Status{
		"considering": core.StatusConsidering, "plan_to_read": core.StatusPlanning, "reading": core.StatusReading,
		"completed": core.StatusCompleted, "paused": core.StatusPaused, "on_hold": core.StatusPaused,
		"dropped": core.StatusDropped, "rereading": core.StatusRereading, "weird": core.StatusUnknown,
	}
	for state, want := range cases {
		if got := statusFromState(state); got != want {
			t.Errorf("statusFromState(%q) = %q, want %q", state, got, want)
		}
	}
	if s, ok := stateFromStatus(core.StatusPlanning); !ok || s != "plan_to_read" {
		t.Errorf("stateFromStatus(planning) = %q, %v", s, ok)
	}
	if _, ok := stateFromStatus(core.StatusUnknown); ok {
		t.Error("unknown must not be writable")
	}
}

func TestGetEntry(t *testing.T) {
	c, _ := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/my/library/1677" {
			io.WriteString(w, `{"status":200,"data":{"series_id":1677,"state":"reading","progress_chapter":104,"progress_volume":null}}`)
			return
		}
		http.NotFound(w, r)
	}))
	e, err := c.GetEntry(t.Context(), "1677")
	if err != nil || e == nil || e.Status != core.StatusReading || *e.Chapter != 104 || e.Volume != nil {
		t.Fatalf("entry = %+v, err = %v", e, err)
	}
	if e, err := c.GetEntry(t.Context(), "999"); e != nil || err != nil {
		t.Fatalf("missing entry = %+v, %v; want nil, nil", e, err)
	}
}

func TestSaveEntryPatchesOnlySetFields(t *testing.T) {
	c, rec := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"status":200,"data":{}}`)
	}))
	ch := 104.5
	if err := c.SaveEntry(t.Context(), "1677", core.EntryUpdate{Chapter: &ch}); err != nil {
		t.Fatal(err)
	}
	got := rec.all()
	if len(got) != 1 || got[0] != `PATCH /v1/my/library/1677 {"progress_chapter":104.5}` {
		t.Fatalf("requests = %q", got)
	}
}

func TestSaveEntryCreatesWhenMissing(t *testing.T) {
	c, rec := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusCreated)
		io.WriteString(w, `{"status":201,"data":{}}`)
	}))
	st := core.StatusReading
	ch := 5.0
	if err := c.SaveEntry(t.Context(), "1677", core.EntryUpdate{Status: &st, Chapter: &ch}); err != nil {
		t.Fatal(err)
	}
	got := rec.all()
	if len(got) != 2 || got[1][:len("POST /v1/my/library/1677")] != "POST /v1/my/library/1677" {
		t.Fatalf("requests = %q", got)
	}
	var body map[string]any
	_ = json.Unmarshal([]byte(got[1][len("POST /v1/my/library/1677 "):]), &body)
	if body["state"] != "reading" || body["progress_chapter"] != 5.0 {
		t.Fatalf("POST body = %v", body)
	}
}

func TestGetEntryParsesDates(t *testing.T) {
	cases := map[string]struct{ body, wantStart, wantFinish string }{
		"date only": {`{"state":"reading","start_date":"2026-06-06","finish_date":"2026-09-01"}`, "2026-06-06", "2026-09-01"},
		"timestamp": {`{"state":"reading","start_date":"2026-06-06T00:00:00.000Z","finish_date":"2026-09-01T00:00:00.000Z"}`, "2026-06-06", "2026-09-01"},
		"null":      {`{"state":"reading","start_date":null,"finish_date":null}`, "", ""},
		"absent":    {`{"state":"reading"}`, "", ""},
		"too short": {`{"state":"reading","start_date":"2026-06","finish_date":""}`, "", ""},
	}
	for name, tc := range cases {
		c, _ := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"status":200,"data":`+tc.body+`}`)
		}))
		e, err := c.GetEntry(t.Context(), "1677")
		if err != nil || e == nil {
			t.Fatalf("%s: entry = %+v, err = %v", name, e, err)
		}
		if e.StartDate != tc.wantStart || e.FinishDate != tc.wantFinish {
			t.Errorf("%s: dates = %q/%q, want %q/%q", name, e.StartDate, e.FinishDate, tc.wantStart, tc.wantFinish)
		}
	}
}

func TestSaveEntrySendsDates(t *testing.T) {
	c, rec := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"status":200,"data":{}}`)
	}))
	start, finish := "2026-06-06", "2026-09-01"
	if err := c.SaveEntry(t.Context(), "1677", core.EntryUpdate{StartDate: &start}); err != nil {
		t.Fatal(err)
	}
	if err := c.SaveEntry(t.Context(), "1677", core.EntryUpdate{StartDate: &start, FinishDate: &finish}); err != nil {
		t.Fatal(err)
	}
	got := rec.all()
	want := []string{
		`PATCH /v1/my/library/1677 {"start_date":"2026-06-06"}`,
		`PATCH /v1/my/library/1677 {"finish_date":"2026-09-01","start_date":"2026-06-06"}`,
	}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("requests = %q, want %q", got, want)
	}
}
