package match

import (
	"slices"
	"sort"
)

// Scored pairs an item with its match score.
type Scored[T any] struct {
	Item  T
	Score float64
}

// Score is the best Similarity between any wanted title and any candidate title. Both sides
// are also compared without a trailing colour-edition marker, so "Naruto (Color)" and
// "Naruto" count as the same series.
func Score(want, have []string) float64 {
	best := 0.0
	have = withEditionVariants(have)
	for _, w := range withEditionVariants(want) {
		for _, h := range have {
			if s := Similarity(w, h); s > best {
				best = s
			}
		}
	}
	return best
}

// Best returns the highest-scoring candidate at or above threshold (first one wins ties),
// plus up to three of the best rejected candidates for logging.
func Best[T any](want []string, candidates []T, titlesOf func(T) []string, threshold float64) (*Scored[T], []Scored[T]) {
	var best *Scored[T]
	var rejected []Scored[T]
	for _, c := range candidates {
		s := Score(want, titlesOf(c))
		if s >= threshold {
			if best == nil || s > best.Score {
				best = &Scored[T]{Item: c, Score: s}
			}
			continue
		}
		rejected = append(rejected, Scored[T]{Item: c, Score: s})
	}
	sort.SliceStable(rejected, func(i, j int) bool { return rejected[i].Score > rejected[j].Score })
	if len(rejected) > 3 {
		rejected = rejected[:3]
	}
	return best, rejected
}

// withEditionVariants returns titles plus, for each title carrying a colour-edition suffix,
// the stripped variant (unless already present).
func withEditionVariants(titles []string) []string {
	out := slices.Clone(titles)
	for _, t := range titles {
		if stripped, ok := StripEdition(t); ok && !slices.Contains(out, stripped) {
			out = append(out, stripped)
		}
	}
	return out
}
