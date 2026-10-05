package httpx

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func fastClient() *Client {
	c := New(5*time.Second, 0)
	c.BaseDelay = time.Millisecond
	return c
}

func TestRetriesServerErrorsAndResendsBody(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if string(b) != `{"a":1}` {
			t.Errorf("attempt %d got body %q", calls.Load()+1, b)
		}
		if r.Header.Get("X-Api-Key") != "k" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("missing headers: %v", r.Header)
		}
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		io.WriteString(w, `{"ok":true}`)
	}))
	defer srv.Close()

	var out struct{ OK bool }
	h := http.Header{}
	h.Set("X-Api-Key", "k")
	if err := fastClient().DoJSON(context.Background(), http.MethodPost, srv.URL, h, map[string]int{"a": 1}, &out); err != nil {
		t.Fatal(err)
	}
	if !out.OK || calls.Load() != 3 {
		t.Fatalf("out=%+v calls=%d", out, calls.Load())
	}
}

func TestNotFoundIsNotRetried(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "nope", http.StatusNotFound)
	}))
	defer srv.Close()

	err := fastClient().DoJSON(context.Background(), http.MethodGet, srv.URL, nil, nil, nil)
	if !IsNotFound(err) || calls.Load() != 1 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
}

func TestGivesUpAfterMaxAttempts(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c := fastClient()
	c.MaxAttempts = 3
	err := c.DoJSON(context.Background(), http.MethodGet, srv.URL, nil, nil, nil)
	if !IsStatus(err, http.StatusTooManyRequests) || calls.Load() != 3 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
}

func TestParseRetryAfter(t *testing.T) {
	cases := map[string]time.Duration{"": 0, "3": 3 * time.Second, "-1": 0, "Wed, 21 Oct 2015 07:28:00 GMT": 0, "3600": maxRetryAfter, "9999999999999": maxRetryAfter}
	for in, want := range cases {
		if got := parseRetryAfter(in); got != want {
			t.Errorf("parseRetryAfter(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestBodyWithoutGetBodyIsNotRetried(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL, io.NopCloser(strings.NewReader("payload")))
	if err != nil {
		t.Fatal(err)
	}
	if req.GetBody != nil {
		t.Fatal("test setup: GetBody should be nil for a NopCloser body")
	}
	resp, err := fastClient().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || calls.Load() != 1 {
		t.Fatalf("status=%d calls=%d, want 503 after exactly 1 call", resp.StatusCode, calls.Load())
	}
}

func TestLimiterWaitReturnsContextError(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer srv.Close()

	c := New(5*time.Second, 1) // 1 per minute, burst 1
	if !c.Limiter.Allow() {    // drain the burst
		t.Fatal("test setup: could not drain limiter")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := c.DoJSON(ctx, http.MethodGet, srv.URL, nil, nil, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("server was called %d times, want 0", calls.Load())
	}
}
