package match

import (
	"testing"

	"mangasync/internal/core"
)

func TestSameSeries(t *testing.T) {
	cases := []struct {
		name string
		a, b core.Series
		want bool
	}{
		{"shared ID equal",
			core.Series{Title: "X", IDs: core.IDs{core.IDAniList: "105778"}},
			core.Series{Title: "Totally different", IDs: core.IDs{core.IDAniList: "105778", core.IDMAL: "1"}},
			true},
		{"shared ID case-insensitive",
			core.Series{IDs: core.IDs{core.IDMangaUpdates: "YLX5WZN"}},
			core.Series{IDs: core.IDs{core.IDMangaUpdates: "ylx5wzn"}},
			true},
		{"shared kind but different ID beats title",
			core.Series{Title: "Naruto", IDs: core.IDs{core.IDAniList: "30011"}},
			core.Series{Title: "Naruto", IDs: core.IDs{core.IDAniList: "99999"}},
			false},
		{"no shared IDs, title match",
			core.Series{Title: "Naruto_ Sasuke's Story - The Uchiha and the Heavenly Stardust_ The Manga"},
			core.Series{Title: "Sasuke Shinden", AltTitles: []string{"Naruto: Sasuke’s Story—The Uchiha and the Heavenly Stardust: The Manga"}, IDs: core.IDs{core.IDAniList: "1"}},
			true},
		{"coloured edition matches original by title",
			core.Series{Title: "Naruto (Color)"},
			core.Series{Title: "NARUTO", IDs: core.IDs{core.IDAniList: "30011"}},
			true},
		{"no shared IDs, different titles",
			core.Series{Title: "Dandadan"},
			core.Series{Title: "Kagurabachi"},
			false},
	}
	for _, c := range cases {
		if got := SameSeries(c.a, c.b, 0.9); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}
