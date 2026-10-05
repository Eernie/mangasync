package komga

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
)

// WatchProgress subscribes to Komga's SSE stream and emits series IDs whose read progress
// changed. Komga only sends read-progress events to the user owning the API key.
func (c *Client) WatchProgress(ctx context.Context) (<-chan string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.cfg.URL+"/sse/v1/events", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-API-Key", c.cfg.APIKey)
	req.Header.Set("Accept", "text/event-stream")
	resp, err := c.stream.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()
		return nil, fmt.Errorf("komga events: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if mt, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type")); err != nil || mt != "text/event-stream" {
		resp.Body.Close()
		return nil, fmt.Errorf("komga events: unexpected Content-Type %q (not an event stream; proxy or SSO page?)", resp.Header.Get("Content-Type"))
	}
	ch := make(chan string, 16)
	go func() {
		defer close(ch)
		defer resp.Body.Close()
		_ = parseEvents(resp.Body, func(event, data string) {
			if event != "ReadProgressSeriesChanged" && event != "ReadProgressSeriesDeleted" {
				return
			}
			var payload struct {
				SeriesID string `json:"seriesId"`
			}
			if json.Unmarshal([]byte(data), &payload) != nil || payload.SeriesID == "" {
				return
			}
			select {
			case ch <- payload.SeriesID:
			case <-ctx.Done():
			}
		})
	}()
	return ch, nil
}

// parseEvents reads a text/event-stream and calls fn for every dispatched event.
func parseEvents(r io.Reader, fn func(event, data string)) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	var event string
	var data []string
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if event != "" || len(data) > 0 {
				fn(event, strings.Join(data, "\n"))
			}
			event, data = "", nil
		case strings.HasPrefix(line, ":"):
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	return sc.Err()
}
