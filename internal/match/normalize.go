// Package match holds the pure title and ID matching shared by sync logic and adapters.
package match

import (
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

// Normalize lowercases s and reduces it to letters, digits and single spaces so that
// differently punctuated titles compare equal. Apostrophes are dropped ("Journey's" ==
// "Journeys"); every other non-alphanumeric rune (including "_" from filesystem names)
// becomes a space. A leading "the" or "a" is dropped.
func Normalize(s string) string {
	s = strings.ToLower(norm.NFKC.String(s))
	var b strings.Builder
	for _, r := range s {
		switch {
		case r == '\'' || r == '’' || r == '‘' || r == '`':
		case unicode.IsLetter(r) || unicode.IsNumber(r):
			b.WriteRune(r)
		default:
			b.WriteByte(' ')
		}
	}
	fields := strings.Fields(b.String())
	if len(fields) > 1 && (fields[0] == "the" || fields[0] == "a") {
		fields = fields[1:]
	}
	return strings.Join(fields, " ")
}

// Similarity returns 1 − Levenshtein distance / longer length of the normalized strings.
// Titles that are equal after removing spaces ("LOSTEND" / "Lost End") score 1.
func Similarity(a, b string) float64 {
	na, nb := Normalize(a), Normalize(b)
	if na == "" || nb == "" {
		return 0
	}
	if na == nb || strings.ReplaceAll(na, " ", "") == strings.ReplaceAll(nb, " ", "") {
		return 1
	}
	ra, rb := []rune(na), []rune(nb)
	return 1 - float64(levenshtein(ra, rb))/float64(max(len(ra), len(rb)))
}

func levenshtein(a, b []rune) int {
	prev := make([]int, len(b)+1)
	cur := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		cur[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev, cur = cur, prev
	}
	return prev[len(b)]
}
