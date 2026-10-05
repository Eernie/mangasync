package suwayomi

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"time"
	"unicode"

	"mangasync/internal/core"
	"mangasync/internal/match"
)

const (
	searchMutation = `mutation($source: LongString!, $q: String!) {
  fetchSourceManga(input: {source: $source, type: SEARCH, page: 1, query: $q}) { mangas { id title inLibrary } } }`
	libraryQuery    = `{ mangas(condition: {inLibrary: true}) { nodes { id title } } }`
	libraryCacheTTL = time.Minute
)

type mangaDTO struct {
	ID        int    `json:"id"`
	Title     string `json:"title"`
	InLibrary bool   `json:"inLibrary"`
}

func mangaTitles(m mangaDTO) []string { return []string{m.Title} }

// searchQueries: the main title, then up to two Latin-script alt titles.
func searchQueries(s core.Series) []string {
	var qs, seen []string
	if s.Title != "" {
		qs = append(qs, s.Title)
		seen = append(seen, match.Normalize(s.Title))
	}
	for _, t := range s.AltTitles {
		if len(qs) >= 3 {
			break
		}
		if n := match.Normalize(t); isLatin(t) && !slices.Contains(seen, n) {
			qs = append(qs, t)
			seen = append(seen, n)
		}
	}
	return qs
}

func isLatin(s string) bool {
	hasLetter := false
	for _, r := range s {
		if unicode.IsLetter(r) {
			if !unicode.Is(unicode.Latin, r) {
				return false
			}
			hasLetter = true
		}
	}
	return hasLetter
}

// Find searches the configured sources in priority order, query by query. The first accepted
// match wins. Source errors are only returned if nothing matched anywhere.
func (c *Client) Find(ctx context.Context, s core.Series) (*core.Candidate, []core.Candidate, error) {
	c.mu.Lock()
	sources := slices.Clone(c.sources)
	c.mu.Unlock()

	var near []core.Candidate
	var errs []error
	for _, q := range searchQueries(s) {
		for _, src := range sources {
			var out struct {
				FetchSourceManga struct {
					Mangas []mangaDTO `json:"mangas"`
				} `json:"fetchSourceManga"`
			}
			if err := c.gql(ctx, searchMutation, map[string]any{"source": src.ID, "q": q}, &out); err != nil {
				if ctx.Err() != nil {
					return nil, sortedNear(near), ctx.Err()
				}
				errs = append(errs, fmt.Errorf("search %s for %q: %w", src.Name, q, err))
				continue
			}
			best, misses := match.Best(s.Titles(), out.FetchSourceManga.Mangas, mangaTitles, c.cfg.Threshold)
			if best != nil {
				return &core.Candidate{Ref: strconv.Itoa(best.Item.ID), SourceName: src.Name, Title: best.Item.Title, Score: best.Score}, nil, nil
			}
			for _, m := range misses {
				near = append(near, core.Candidate{Ref: strconv.Itoa(m.Item.ID), SourceName: src.Name, Title: m.Item.Title, Score: m.Score})
			}
		}
	}
	return nil, sortedNear(near), errors.Join(errs...)
}

// sortedNear returns the three best-scoring near misses.
func sortedNear(near []core.Candidate) []core.Candidate {
	sort.SliceStable(near, func(i, j int) bool { return near[i].Score > near[j].Score })
	if len(near) > 3 {
		near = near[:3]
	}
	return near
}

// FindInLibrary matches s against the Suwayomi library (cached for a minute).
func (c *Client) FindInLibrary(ctx context.Context, s core.Series) (*core.Candidate, error) {
	lib, err := c.library(ctx)
	if err != nil {
		return nil, err
	}
	best, _ := match.Best(s.Titles(), lib, mangaTitles, c.cfg.Threshold)
	if best == nil {
		return nil, nil
	}
	return &core.Candidate{Ref: strconv.Itoa(best.Item.ID), Title: best.Item.Title, Score: best.Score}, nil
}

func (c *Client) library(ctx context.Context) ([]mangaDTO, error) {
	c.mu.Lock()
	if c.lib != nil && c.now().Sub(c.libAt) < libraryCacheTTL {
		lib := c.lib
		c.mu.Unlock()
		return lib, nil
	}
	c.mu.Unlock()

	var out struct {
		Mangas struct {
			Nodes []mangaDTO `json:"nodes"`
		} `json:"mangas"`
	}
	if err := c.gql(ctx, libraryQuery, nil, &out); err != nil {
		return nil, fmt.Errorf("list library: %w", err)
	}
	lib := out.Mangas.Nodes
	if lib == nil {
		lib = []mangaDTO{}
	}
	c.mu.Lock()
	c.lib, c.libAt = lib, c.now()
	c.mu.Unlock()
	return lib, nil
}

func (c *Client) invalidateLibrary() {
	c.mu.Lock()
	c.lib = nil
	c.mu.Unlock()
}
