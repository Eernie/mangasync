package suwayomi

import (
	"testing"

	"mangasync/internal/core"
)

const weeb = "2131019126180322627"
const manhuatop = "1903782575226230108"

func TestFindFallsThroughSourcesInOrder(t *testing.T) {
	f := newFakeServer(t)
	f.search[weeb] = map[string]string{"Kagurabachi": `[{"id":1,"title":"Kagura Bachi Gaiden","inLibrary":false}]`}
	f.search[manhuatop] = map[string]string{"Kagurabachi": `[{"id":7,"title":"Kagurabachi","inLibrary":false}]`}
	c := newTestClient(t, f, Config{})

	best, _, err := c.Find(t.Context(), core.Series{Title: "Kagurabachi"})
	if err != nil || best == nil || best.Ref != "7" || best.SourceName != "ManhuaTop (EN)" {
		t.Fatalf("best = %+v, err = %v", best, err)
	}
}

func TestFindUsesLatinAltTitles(t *testing.T) {
	f := newFakeServer(t)
	f.search[weeb] = map[string]string{
		"Frieren: Beyond Journey’s End": `[{"id":130,"title":"Frieren - Beyond Journey's End","inLibrary":false}]`,
	}
	c := newTestClient(t, f, Config{})
	s := core.Series{Title: "Sousou no Frieren", AltTitles: []string{"葬送のフリーレン", "Frieren: Beyond Journey’s End"}}

	best, _, err := c.Find(t.Context(), s)
	if err != nil || best == nil || best.Ref != "130" {
		t.Fatalf("best = %+v, err = %v", best, err)
	}
	for _, call := range f.callsMatching("fetchSourceManga") {
		if call.Variables["q"] == "葬送のフリーレン" {
			t.Fatal("non-Latin titles must not be used as queries")
		}
	}
}

func TestFindNoMatchReturnsNearMisses(t *testing.T) {
	f := newFakeServer(t)
	f.search[weeb] = map[string]string{"Obscure": `[{"id":1,"title":"Obscura","inLibrary":false},{"id":2,"title":"Zzz","inLibrary":false}]`}
	c := newTestClient(t, f, Config{})

	best, near, err := c.Find(t.Context(), core.Series{Title: "Obscure"})
	if err != nil || best != nil || len(near) == 0 || near[0].Title != "Obscura" {
		t.Fatalf("best = %+v, near = %+v, err = %v", best, near, err)
	}
}

func TestFindSourceErrorDoesNotHideOtherSources(t *testing.T) {
	f := newFakeServer(t)
	f.errorFor[weeb] = "cloudflare challenge"
	f.search[manhuatop] = map[string]string{"Kagurabachi": `[{"id":7,"title":"Kagurabachi","inLibrary":false}]`}
	c := newTestClient(t, f, Config{})

	best, _, err := c.Find(t.Context(), core.Series{Title: "Kagurabachi"})
	if err != nil || best == nil || best.Ref != "7" {
		t.Fatalf("best = %+v, err = %v", best, err)
	}

	// With no match anywhere, the source error is returned so the caller retries instead of recording not_found.
	_, _, err = c.Find(t.Context(), core.Series{Title: "Nothing"})
	if err == nil {
		t.Fatal("expected source error when nothing matched")
	}
}

func TestFindInLibraryIsCached(t *testing.T) {
	f := newFakeServer(t)
	f.library = `[{"id":24,"title":"Witch Hat Atelier"},{"id":33,"title":"Chainsaw Man"}]`
	c := newTestClient(t, f, Config{})

	got, err := c.FindInLibrary(t.Context(), core.Series{Title: "Chainsaw Man"})
	if err != nil || got == nil || got.Ref != "33" {
		t.Fatalf("got %+v, %v", got, err)
	}
	got, _ = c.FindInLibrary(t.Context(), core.Series{Title: "Frieren"})
	if got != nil {
		t.Fatalf("unexpected match %+v", got)
	}
	if n := len(f.callsMatching("mangas(condition")); n != 1 {
		t.Fatalf("library queries = %d, want 1 (cached)", n)
	}
}
