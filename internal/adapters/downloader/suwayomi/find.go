package suwayomi

import "context"

type mangaDTO struct {
	ID    int    `json:"id"`
	Title string `json:"title"`
}

func (c *Client) library(ctx context.Context) ([]mangaDTO, error) {
	var out struct {
		Mangas struct {
			Nodes []mangaDTO `json:"nodes"`
		} `json:"mangas"`
	}
	err := c.gql(ctx, `{ mangas(condition: {inLibrary: true}) { nodes { id title } } }`, nil, &out)
	return out.Mangas.Nodes, err
}
