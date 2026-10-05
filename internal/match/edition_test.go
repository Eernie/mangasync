package match

import "testing"

func TestStripEdition(t *testing.T) {
	cases := []struct {
		in   string
		want string
		ok   bool
	}{
		{"Naruto (Color)", "Naruto", true},
		{"Naruto [Colored]", "Naruto", true},
		{"One Piece (Official Colored)", "One Piece", true},
		{"Dragon Ball (Digital Colored Comics)", "Dragon Ball", true},
		{"JoJo (Full Color Edition)", "JoJo", true},
		{"Akira (colour)", "Akira", true},
		{"Bleach (COLOURED VERSION)  ", "Bleach", true},
		{"Naruto ", "Naruto ", false},
		{"  Naruto", "  Naruto", false},
		{"Re:Zero (Arc 2)", "Re:Zero (Arc 2)", false},
		{"Kaiju No. 8 (2020)", "Kaiju No. 8 (2020)", false},
		{"Naruto Color Edition", "Naruto Color Edition", false},
		{"(Color)", "(Color)", false},
		{"The Color of Magic", "The Color of Magic", false},
	}
	for _, tc := range cases {
		got, ok := StripEdition(tc.in)
		if got != tc.want || ok != tc.ok {
			t.Errorf("StripEdition(%q) = %q, %v; want %q, %v", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}
