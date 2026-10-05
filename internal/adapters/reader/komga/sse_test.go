package komga

import (
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"mangasync/internal/httpx"
)

const stream = "event:TaskQueueStatus\ndata:{\"count\":0,\"countByType\":{}}\n\n" +
	": keep-alive comment\n\n" +
	"event:ReadProgressSeriesChanged\ndata:{\"seriesId\":\"S1\",\"userId\":\"U\"}\n\n" +
	"event:ReadProgressChanged\ndata:{\"bookId\":\"B1\",\"userId\":\"U\"}\n\n" +
	"event:ReadProgressSeriesDeleted\ndata:{\"seriesId\":\"S2\",\"userId\":\"U\"}\n\n" +
	"event:SeriesChanged\ndata:{\"seriesId\":\"S9\",\"libraryId\":\"L1\"}\n\n"

func TestParseEvents(t *testing.T) {
	var got []string
	err := parseEvents(strings.NewReader(stream), func(event, data string) { got = append(got, event+"|"+data) })
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 || got[1] != `ReadProgressSeriesChanged|{"seriesId":"S1","userId":"U"}` {
		t.Fatalf("got %q", got)
	}
}

func TestWatchProgress(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sse/v1/events" || r.Header.Get("X-API-Key") != "secret" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, stream)
		w.(http.Flusher).Flush()
	}))
	defer srv.Close()

	ch, err := New(Config{URL: srv.URL, APIKey: "secret"}, httpx.New(5*time.Second, 0)).WatchProgress(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var refs []string
	for ref := range ch { // closes when the server ends the stream
		refs = append(refs, ref)
	}
	if !slices.Equal(refs, []string{"S1", "S2"}) {
		t.Fatalf("refs = %v", refs)
	}

	if _, err := New(Config{URL: srv.URL, APIKey: "wrong"}, httpx.New(5*time.Second, 0)).WatchProgress(t.Context()); err == nil {
		t.Fatal("expected error for rejected key")
	}
}

func TestWatchProgressRejectsNonEventStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		io.WriteString(w, "<html>login</html>")
	}))
	defer srv.Close()
	ch, err := New(Config{URL: srv.URL, APIKey: "secret"}, httpx.New(5*time.Second, 0)).WatchProgress(t.Context())
	if err == nil {
		t.Fatalf("expected error for 200 text/html, got channel %v", ch)
	}
}
