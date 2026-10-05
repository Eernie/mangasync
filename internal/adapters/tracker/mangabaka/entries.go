package mangabaka

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"mangasync/internal/core"
	"mangasync/internal/httpx"
)

type entryDTO struct {
	SeriesID        int      `json:"series_id"`
	State           string   `json:"state"`
	ProgressChapter *float64 `json:"progress_chapter"`
	ProgressVolume  *float64 `json:"progress_volume"`
}

func (c *Client) GetEntry(ctx context.Context, id string) (*core.Entry, error) {
	var env struct {
		Data entryDTO `json:"data"`
	}
	err := c.api.DoJSON(ctx, http.MethodGet, c.url("/v1/my/library/"+url.PathEscape(id)), c.header(), nil, &env)
	if httpx.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &core.Entry{Status: statusFromState(env.Data.State), Chapter: env.Data.ProgressChapter, Volume: env.Data.ProgressVolume}, nil
}

// SaveEntry PATCHes the entry, creating it with POST if it is not in the library yet.
func (c *Client) SaveEntry(ctx context.Context, id string, u core.EntryUpdate) error {
	body := map[string]any{}
	if u.Status != nil {
		state, ok := stateFromStatus(*u.Status)
		if !ok {
			return fmt.Errorf("mangabaka: cannot write status %q", *u.Status)
		}
		body["state"] = state
	}
	if u.Chapter != nil {
		body["progress_chapter"] = *u.Chapter
	}
	if u.Volume != nil {
		body["progress_volume"] = *u.Volume
	}
	if len(body) == 0 {
		return nil
	}
	path := c.url("/v1/my/library/" + url.PathEscape(id))
	err := c.api.DoJSON(ctx, http.MethodPatch, path, c.header(), body, nil)
	if !httpx.IsNotFound(err) {
		return err
	}
	if _, ok := body["state"]; !ok {
		body["state"] = "reading"
	}
	return c.api.DoJSON(ctx, http.MethodPost, path, c.header(), body, nil)
}
