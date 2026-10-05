package suwayomi

import (
	"fmt"
	"testing"

	"mangasync/internal/core"
	"mangasync/internal/core/coretest"
)

func TestAcquireAddsAndQueuesMissingChapters(t *testing.T) {
	f := newFakeServer(t)
	f.chapters = `[{"id":1,"isDownloaded":true},{"id":2,"isDownloaded":false},{"id":3,"isDownloaded":false}]`
	c := newTestClient(t, f, Config{})
	c.lib, c.libAt = []mangaDTO{}, c.now() // warm cache that Acquire must invalidate

	if err := c.Acquire(t.Context(), core.Candidate{Ref: "130"}); err != nil {
		t.Fatal(err)
	}
	upd := f.callsMatching("updateManga")
	if len(upd) != 1 || upd[0].Variables["id"] != 130.0 || upd[0].Variables["in"] != true {
		t.Fatalf("updateManga calls = %+v", upd)
	}
	fetch := f.callsMatching("fetchMangaAndChapters")
	if len(fetch) != 1 || fetch[0].Variables["id"] != 130.0 {
		t.Fatalf("fetch calls = %+v", fetch)
	}
	enq := f.callsMatching("enqueueChapterDownloads")
	if len(enq) != 1 || fmt.Sprint(enq[0].Variables["ids"]) != "[2 3]" {
		t.Fatalf("enqueue calls = %+v", enq)
	}
	if c.lib != nil {
		t.Fatal("library cache not invalidated")
	}
}

func TestAcquireAllDownloadedSkipsEnqueue(t *testing.T) {
	f := newFakeServer(t)
	f.chapters = `[{"id":1,"isDownloaded":true}]`
	c := newTestClient(t, f, Config{})
	if err := c.Acquire(t.Context(), core.Candidate{Ref: "5"}); err != nil {
		t.Fatal(err)
	}
	if n := len(f.callsMatching("enqueueChapterDownloads")); n != 0 {
		t.Fatalf("enqueue calls = %d, want 0", n)
	}
}

func TestRelease(t *testing.T) {
	f := newFakeServer(t)
	c := newTestClient(t, f, Config{})
	if err := c.Release(t.Context(), core.Candidate{Ref: "50"}); err != nil {
		t.Fatal(err)
	}
	upd := f.callsMatching("updateManga")
	if len(upd) != 1 || upd[0].Variables["id"] != 50.0 || upd[0].Variables["in"] != false {
		t.Fatalf("updateManga calls = %+v", upd)
	}
}

func TestBadRef(t *testing.T) {
	c := newTestClient(t, newFakeServer(t), Config{})
	if err := c.Acquire(t.Context(), core.Candidate{Ref: "abc"}); err == nil {
		t.Fatal("expected error for non-numeric ref")
	}
}

func TestContract(t *testing.T) {
	f := newFakeServer(t)
	f.search[weeb] = map[string]string{"Chainsaw Man": `[{"id":33,"title":"Chainsaw Man","inLibrary":true}]`}
	coretest.DownloaderContract(t, newTestClient(t, f, Config{}), core.Series{Title: "Chainsaw Man"})
}
