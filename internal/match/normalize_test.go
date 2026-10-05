package match

import (
	"math"
	"testing"
)

func TestNormalize(t *testing.T) {
	cases := map[string]string{
		"Naruto_ Sasuke's Story - The Uchiha and the Heavenly Stardust_ The Manga": "naruto sasukes story the uchiha and the heavenly stardust the manga",
		"Naruto: Sasuke’s Story—The Uchiha and the Heavenly Stardust: The Manga":   "naruto sasukes story the uchiha and the heavenly stardust the manga",
		"Frieren - Beyond Journey's End":                                           "frieren beyond journeys end",
		"Frieren: Beyond Journey’s End":                                            "frieren beyond journeys end",
		"The Promised Neverland":                                                   "promised neverland",
		"Dr. STONE":                                                                "dr stone",
		"  Chainsaw   Man ":                                                        "chainsaw man",
		"Ｃｈａｉｎｓａｗ Man":                                                             "chainsaw man",
		"A":                                                                        "a",
		"[Oshi no Ko]":                                                             "oshi no ko",
		"":                                                                         "",
		"Sōsō no Frieren":                                                          "soso no frieren",
		"Ōoku":                                                                     "ooku",
		"Ooku":                                                                     "ooku",
		"ガンダム":                                                                     "ガンダム",
	}
	for in, want := range cases {
		if got := Normalize(in); got != want {
			t.Errorf("Normalize(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSimilarity(t *testing.T) {
	cases := []struct {
		a, b string
		want float64
	}{
		{"Frieren - Beyond Journey's End", "Frieren: Beyond Journey’s End", 1},
		{"LOSTEND", "Lost End", 1},
		{"Chainsaw Man", "Chainsaw Men", 1 - 1.0/12},
		{"abc", "xyz", 0},
		{"", "abc", 0},
	}
	for _, c := range cases {
		if got := Similarity(c.a, c.b); math.Abs(got-c.want) > 1e-9 {
			t.Errorf("Similarity(%q, %q) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}

func TestSimilarityNumberGuard(t *testing.T) {
	differ := [][2]string{
		{"Kaiju No. 8", "Kaiju No. 9"},
		{"Re:Zero Chapter 2", "Re:Zero Chapter 3"},
		{"Kingdom Hearts II", "Kingdom Hearts III"},
		{"JoJo's Bizarre Adventure Part 4", "JoJo's Bizarre Adventure Part 5"},
		{"Mob Psycho 100", "Mob Psycho"},
	}
	for _, c := range differ {
		if got := Similarity(c[0], c[1]); got >= 0.9 {
			t.Errorf("Similarity(%q, %q) = %v, want < 0.9", c[0], c[1], got)
		}
	}
	same := [][2]string{
		{"Hunter x Hunter", "Hunter X Hunter"},
		{"Kaiju No. 8", "Kaiju No.8"},
	}
	for _, c := range same {
		if got := Similarity(c[0], c[1]); got != 1 {
			t.Errorf("Similarity(%q, %q) = %v, want 1", c[0], c[1], got)
		}
	}
}

func TestSimilarityFoldsAccents(t *testing.T) {
	if got := Similarity("Ooku", "Ōoku"); got != 1 {
		t.Errorf("Similarity(Ooku, Ōoku) = %v, want 1", got)
	}
}
