package match

import "testing"

type cand struct {
	id    int
	title string
}

func titlesOf(c cand) []string { return []string{c.title} }

func TestBestPicksHighestAccepted(t *testing.T) {
	candidates := []cand{
		{84642, "Naruto Retsuden"},
		{374014, "Sasuke's Story: The Uchiha and the Heavenly Stardust"},
		{56702, "Naruto: Sasuke’s Story—The Uchiha and the Heavenly Stardust: The Manga"},
	}
	want := []string{"Naruto_ Sasuke's Story - The Uchiha and the Heavenly Stardust_ The Manga"}
	best, near := Best(want, candidates, titlesOf, 0.9)
	if best == nil || best.Item.id != 56702 || best.Score != 1 {
		t.Fatalf("best = %+v, want id 56702 with score 1", best)
	}
	if len(near) != 2 || near[0].Score < near[1].Score {
		t.Fatalf("near misses should be the 2 rejected, sorted desc: %+v", near)
	}
}

func TestBestUsesAltTitles(t *testing.T) {
	want := []string{"Sousou no Frieren", "Frieren: Beyond Journey’s End"}
	best, _ := Best(want, []cand{{130, "Frieren - Beyond Journey's End"}}, titlesOf, 0.9)
	if best == nil || best.Item.id != 130 {
		t.Fatalf("expected match via alt title, got %+v", best)
	}
}

func TestBestNoMatchReturnsAtMostThreeNearMisses(t *testing.T) {
	candidates := []cand{{1, "aaaa"}, {2, "bbbb"}, {3, "cccc"}, {4, "dddd"}, {5, "Chainsaw Men"}}
	best, near := Best([]string{"Chainsaw Man"}, candidates, titlesOf, 0.95)
	if best != nil {
		t.Fatalf("expected no match, got %+v", best)
	}
	if len(near) != 3 || near[0].Item.id != 5 {
		t.Fatalf("near misses = %+v, want 3 with id 5 first", near)
	}
}
