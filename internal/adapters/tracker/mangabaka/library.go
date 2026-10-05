package mangabaka

import (
	"context"
	"net/http"
	"net/url"
	"strconv"

	"mangasync/internal/core"
)

// ListLibrary returns the user's library entries with one of the given statuses.
// Uses /v2/my/library: v1 list items do not carry the series ID. One state is requested at a
// time because it is unverified whether the API honours repeated `state` params; a silently
// ignored value would drop entries from the result.
func (c *Client) ListLibrary(ctx context.Context, statuses []core.Status) ([]core.LibraryEntry, error) {
	var out []core.LibraryEntry
	seen := map[string]bool{}
	for _, st := range statuses {
		state, ok := stateFromStatus(st)
		if !ok {
			continue
		}
		entries, err := c.listState(ctx, st, state)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if seen[e.Series.Ref] {
				continue
			}
			seen[e.Series.Ref] = true
			out = append(out, e)
		}
	}
	return out, nil
}

// listState fetches every page of the library for a single MangaBaka state.
func (c *Client) listState(ctx context.Context, want core.Status, state string) ([]core.LibraryEntry, error) {
	const limit = 100
	q := url.Values{"limit": {strconv.Itoa(limit)}, "schema": {"full"}, "state": {state}}
	var out []core.LibraryEntry
	for page := 1; ; page++ {
		q.Set("page", strconv.Itoa(page))
		var env struct {
			Data []struct {
				Entry  entryDTO  `json:"entry"`
				Series seriesDTO `json:"series"`
			} `json:"data"`
			Pagination struct {
				Next  *string `json:"next"`
				Count int     `json:"count"`
			} `json:"pagination"`
		}
		if err := c.api.DoJSON(ctx, http.MethodGet, c.url("/v2/my/library?"+q.Encode()), c.header(), nil, &env); err != nil {
			return nil, err
		}
		for _, it := range env.Data {
			st := statusFromState(it.Entry.State)
			if st != want {
				continue
			}
			out = append(out, core.LibraryEntry{Series: it.Series.toCore(), Status: st})
		}
		if env.Pagination.Next == nil || len(env.Data) == 0 ||
			(env.Pagination.Count > 0 && page*limit >= env.Pagination.Count) {
			return out, nil
		}
	}
}
