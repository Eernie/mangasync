package app

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestQueueDedupsAndKeepsOrder(t *testing.T) {
	q := NewQueue()
	q.Push("a")
	q.Push("b")
	q.Push("a")
	for _, want := range []string{"a", "b"} {
		got, ok := q.Pop(t.Context())
		if !ok || got != want {
			t.Fatalf("Pop = %q, %v; want %q", got, ok, want)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, ok := q.Pop(ctx); ok {
		t.Fatal("Pop on empty queue should block until ctx is done")
	}
}

func TestQueuePopWakesOnPush(t *testing.T) {
	q := NewQueue()
	go func() {
		time.Sleep(10 * time.Millisecond)
		q.Push("x")
	}()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if got, ok := q.Pop(ctx); !ok || got != "x" {
		t.Fatalf("Pop = %q, %v", got, ok)
	}
}

func TestDebouncerCoalesces(t *testing.T) {
	var mu sync.Mutex
	fired := map[string]int{}
	d := NewDebouncer(30*time.Millisecond, func(ref string) {
		mu.Lock()
		fired[ref]++
		mu.Unlock()
	})
	defer d.Stop()
	for range 5 {
		d.Trigger("s1")
		time.Sleep(5 * time.Millisecond)
	}
	d.Trigger("s2")
	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if fired["s1"] != 1 || fired["s2"] != 1 {
		t.Fatalf("fired = %v; want each once", fired)
	}
}
