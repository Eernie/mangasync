// Package httpx is the HTTP client every adapter uses: rate limiting, retries with
// backoff on network errors / 429 / 5xx, and JSON helpers.
package httpx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"

	"golang.org/x/time/rate"
)

// StatusError is returned by DoJSON for non-2xx responses.
type StatusError struct {
	Method string
	URL    string
	Code   int
	Body   string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("%s %s: HTTP %d: %s", e.Method, e.URL, e.Code, e.Body)
}

func IsStatus(err error, code int) bool {
	var se *StatusError
	return errors.As(err, &se) && se.Code == code
}

func IsNotFound(err error) bool { return IsStatus(err, http.StatusNotFound) }

// maxRetryAfter caps how long a server-supplied Retry-After can stall a request.
const maxRetryAfter = 2 * time.Minute

type Client struct {
	HTTP        *http.Client
	Limiter     *rate.Limiter // nil = unlimited
	MaxAttempts int
	BaseDelay   time.Duration
}

// New returns a client with the given request timeout. perMinute <= 0 disables rate limiting.
func New(timeout time.Duration, perMinute int) *Client {
	c := &Client{HTTP: &http.Client{Timeout: timeout}, MaxAttempts: 5, BaseDelay: 500 * time.Millisecond}
	if perMinute > 0 {
		c.Limiter = rate.NewLimiter(rate.Limit(float64(perMinute)/60), 1)
	}
	return c
}

// Do sends req, retrying network errors, 429 and 5xx with exponential backoff and jitter.
// Retry-After (seconds) is honoured. The final response is returned as-is, whatever its status.
// Requests with a body must have GetBody set (http.NewRequest does this for bytes.Reader).
func (c *Client) Do(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	attempts := max(c.MaxAttempts, 1)
	if req.Body != nil && req.Body != http.NoBody && req.GetBody == nil {
		attempts = 1 // the body cannot be re-sent
	}
	var lastErr error
	var retryAfter time.Duration
	for i := range attempts {
		if i > 0 {
			if err := sleep(ctx, c.backoff(i, retryAfter)); err != nil {
				return nil, err
			}
		}
		if c.Limiter != nil {
			if err := c.Limiter.Wait(ctx); err != nil {
				return nil, c.limiterError(ctx, err)
			}
		}
		attempt := req.Clone(ctx)
		if req.GetBody != nil {
			body, err := req.GetBody()
			if err != nil {
				return nil, err
			}
			attempt.Body = body
		}
		resp, err := c.HTTP.Do(attempt)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			lastErr, retryAfter = err, 0
			continue
		}
		retryable := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
		if retryable && i < attempts-1 {
			retryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			lastErr = fmt.Errorf("%s %s: HTTP %d", req.Method, req.URL, resp.StatusCode)
			continue
		}
		return resp, nil
	}
	return nil, lastErr
}

// DoJSON sends in (if non-nil) as JSON and decodes a 2xx response into out (if non-nil).
// Non-2xx responses become *StatusError.
func (c *Client) DoJSON(ctx context.Context, method, url string, header http.Header, in, out any) error {
	var body io.Reader = http.NoBody
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return err
	}
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	req.Header.Set("Accept", "application/json")
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return &StatusError{Method: method, URL: url, Code: resp.StatusCode, Body: strings.TrimSpace(string(b))}
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode %s %s: %w", method, url, err)
	}
	return nil
}

// limiterError maps a failed Limiter.Wait to the context's error so callers can detect
// shutdown or a deadline with errors.Is. Wait also fails early, before the context is done,
// when the wait would outlast the deadline; that is reported as context.DeadlineExceeded.
func (c *Client) limiterError(ctx context.Context, err error) error {
	if cerr := ctx.Err(); cerr != nil {
		return cerr
	}
	if _, ok := ctx.Deadline(); ok && c.Limiter.Burst() >= 1 {
		return fmt.Errorf("%w: %v", context.DeadlineExceeded, err)
	}
	return err
}

func (c *Client) backoff(attempt int, retryAfter time.Duration) time.Duration {
	d := c.BaseDelay << (attempt - 1)
	if c.BaseDelay > 0 {
		d += rand.N(c.BaseDelay)
	}
	return max(d, retryAfter)
}

func parseRetryAfter(v string) time.Duration {
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || n < 0 {
		return 0
	}
	if n > int(maxRetryAfter/time.Second) { // also avoids overflow in the multiplication below
		return maxRetryAfter
	}
	return time.Duration(n) * time.Second
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
