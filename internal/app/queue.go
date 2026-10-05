// Package app runs the sync loops.
package app

import (
	"context"
	"sync"
	"time"
)

// Queue is a FIFO of series refs that ignores refs already waiting.
type Queue struct {
	mu      sync.Mutex
	pending []string
	queued  map[string]bool
	notify  chan struct{}
}

func NewQueue() *Queue {
	return &Queue{queued: map[string]bool{}, notify: make(chan struct{}, 1)}
}

func (q *Queue) Push(ref string) {
	q.mu.Lock()
	if !q.queued[ref] {
		q.queued[ref] = true
		q.pending = append(q.pending, ref)
	}
	q.mu.Unlock()
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// Pop blocks until a ref is available or ctx is done.
func (q *Queue) Pop(ctx context.Context) (string, bool) {
	for {
		q.mu.Lock()
		if len(q.pending) > 0 {
			ref := q.pending[0]
			q.pending = q.pending[1:]
			delete(q.queued, ref)
			q.mu.Unlock()
			return ref, true
		}
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return "", false
		case <-q.notify:
		}
	}
}

// Debouncer calls fire(ref) once a ref has been quiet for delay.
type Debouncer struct {
	delay  time.Duration
	fire   func(string)
	mu     sync.Mutex
	timers map[string]*time.Timer
}

func NewDebouncer(delay time.Duration, fire func(string)) *Debouncer {
	return &Debouncer{delay: delay, fire: fire, timers: map[string]*time.Timer{}}
}

func (d *Debouncer) Trigger(ref string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if t, ok := d.timers[ref]; ok {
		t.Stop()
	}
	var t *time.Timer
	t = time.AfterFunc(d.delay, func() {
		d.mu.Lock()
		if d.timers[ref] == t {
			delete(d.timers, ref)
		}
		d.mu.Unlock()
		d.fire(ref)
	})
	d.timers[ref] = t
}

// Stop cancels all pending timers.
func (d *Debouncer) Stop() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for ref, t := range d.timers {
		t.Stop()
		delete(d.timers, ref)
	}
}
