package suwayomi

import (
	"context"
	"fmt"
	"strconv"

	"mangasync/internal/core"
)

var _ core.Downloader = (*Client)(nil)

const (
	setInLibraryMutation = `mutation($id: Int!, $in: Boolean!) {
  updateManga(input: {id: $id, patch: {inLibrary: $in}}) { manga { id } } }`
	fetchChaptersMutation = `mutation($id: Int!) {
  fetchMangaAndChapters(input: {id: $id, fetchManga: true, fetchChapters: true}) { chapters { id isDownloaded } } }`
	enqueueMutation = `mutation($ids: [Int!]!) { enqueueChapterDownloads(input: {ids: $ids}) { clientMutationId } }`
)

// Acquire adds the manga to the library, refreshes its chapter list and queues every
// chapter that is not downloaded yet.
func (c *Client) Acquire(ctx context.Context, cand core.Candidate) error {
	id, err := strconv.Atoi(cand.Ref)
	if err != nil {
		return fmt.Errorf("suwayomi: bad manga ref %q", cand.Ref)
	}
	if err := c.setInLibrary(ctx, id, true); err != nil {
		return fmt.Errorf("add to library: %w", err)
	}
	var out struct {
		FetchMangaAndChapters struct {
			Chapters []struct {
				ID           int  `json:"id"`
				IsDownloaded bool `json:"isDownloaded"`
			} `json:"chapters"`
		} `json:"fetchMangaAndChapters"`
	}
	if err := c.gql(ctx, fetchChaptersMutation, map[string]any{"id": id}, &out); err != nil {
		return fmt.Errorf("fetch chapters: %w", err)
	}
	var ids []int
	for _, ch := range out.FetchMangaAndChapters.Chapters {
		if !ch.IsDownloaded {
			ids = append(ids, ch.ID)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	if err := c.gql(ctx, enqueueMutation, map[string]any{"ids": ids}, nil); err != nil {
		return fmt.Errorf("enqueue %d chapters: %w", len(ids), err)
	}
	return nil
}

// Release removes the manga from the library. Downloaded chapters stay on disk.
func (c *Client) Release(ctx context.Context, cand core.Candidate) error {
	id, err := strconv.Atoi(cand.Ref)
	if err != nil {
		return fmt.Errorf("suwayomi: bad manga ref %q", cand.Ref)
	}
	return c.setInLibrary(ctx, id, false)
}

func (c *Client) setInLibrary(ctx context.Context, id int, in bool) error {
	defer c.invalidateLibrary()
	return c.gql(ctx, setInLibraryMutation, map[string]any{"id": id, "in": in}, nil)
}
