package mangabaka

import (
	"context"
	"net/http"
	"net/url"
	"slices"
	"strconv"

	"mangasync/internal/core"
)

// ListLibrary returns the user's library entries with one of the given statuses.
// Uses /v2/my/library: v1 list items do not carry the series ID.
func (c *Client) ListLibrary(ctx context.Context, statuses []core.Status) ([]core.LibraryEntry, error) {
	const limit = 100
	q := url.Values{"limit": {strconv.Itoa(limit)}, "schema": {"full"}}
	for _, st := range statuses {
		if state, ok := stateFromStatus(st); ok {
			q.Add("state", state)
		}
	}
	if len(q["state"]) == 0 {
		return nil, nil
	}
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
			if !slices.Contains(statuses, st) {
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
