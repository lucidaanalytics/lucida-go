package lucida

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

const maxBatch = 1000

// sendFunc posts one batch and returns the HTTP status (0 on network error) and any Retry-After.
type sendFunc func(batch []wireEvent) (status int, retryAfter time.Duration)

// queue is a bounded batching queue drained by one background goroutine: flush at flushAt events
// or interval after the first queued event; retries with backoff; drops the oldest when full.
type queue struct {
	send                    sendFunc
	logf                    func(string, ...any)
	flushAt, maxSize, tries int
	batchSize               int                                 // events per request, <= maxBatch
	onDrop                  func(batch []wireEvent, status int) // optional: a batch was given up on
	interval                time.Duration
	backoff                 func(attempt int, retryAfter time.Duration) time.Duration

	mu      sync.Mutex
	items   []wireEvent
	closed  bool
	dropped atomic.Int64

	wake   chan struct{}      // buffered 1: "re-evaluate"
	flushC chan chan struct{} // flush request; closed when drained
	stop   chan struct{}      // closed on shutdown: abandons retries
	done   chan struct{}
	once   sync.Once
}

func newQueue(send sendFunc, flushAt, maxSize, retries, batchSize int, interval time.Duration, logf func(string, ...any),
	onDrop func([]wireEvent, int)) *queue {
	q := &queue{
		send: send, logf: logf, flushAt: flushAt, maxSize: maxSize, tries: retries, interval: interval, backoff: backoff,
		batchSize: max(1, min(batchSize, maxBatch)), onDrop: onDrop,
		wake: make(chan struct{}, 1), flushC: make(chan chan struct{}), stop: make(chan struct{}), done: make(chan struct{}),
	}
	go q.run()
	return q
}

func (q *queue) add(e wireEvent) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.items = append(q.items, e)
	if len(q.items) > q.maxSize {
		q.items = append(q.items[:0], q.items[1:]...)
		q.dropped.Add(1)
	}
	n := len(q.items)
	q.mu.Unlock()
	if n == 1 || n == q.flushAt {
		select {
		case q.wake <- struct{}{}:
		default:
		}
	}
}

func (q *queue) size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

func (q *queue) run() {
	defer close(q.done)
	for {
		n := q.size()
		if n >= q.flushAt {
			q.sendBatch()
			continue
		}
		var timer <-chan time.Time
		var t *time.Timer
		if n > 0 {
			t = time.NewTimer(q.interval)
			timer = t.C
		}
		select {
		case <-q.wake:
		case <-timer:
			q.drain()
		case r := <-q.flushC:
			q.drain()
			close(r)
		case <-q.stop:
			if t != nil {
				t.Stop()
			}
			return
		}
		if t != nil {
			t.Stop()
		}
	}
}

func (q *queue) drain() {
	for q.size() > 0 {
		select {
		case <-q.stop:
			return
		default:
		}
		q.sendBatch()
	}
}

func (q *queue) sendBatch() {
	q.mu.Lock()
	n := min(q.batchSize, len(q.items))
	batch := append([]wireEvent(nil), q.items[:n]...)
	q.items = append(q.items[:0], q.items[n:]...)
	q.mu.Unlock()
	for attempt := 0; ; attempt++ {
		status, ra := q.send(batch)
		if status >= 200 && status < 300 {
			return
		}
		if !retryable(status) || attempt >= q.tries {
			q.dropBatch(batch, status)
			return
		}
		d := q.backoff(attempt, ra)
		q.logf("retry in %s (status %d)", d, status)
		select {
		case <-time.After(d):
		case <-q.stop:
			q.dropBatch(batch, status)
			return
		}
	}
}

func (q *queue) dropBatch(batch []wireEvent, status int) {
	q.dropped.Add(int64(len(batch)))
	q.logf("dropping %d events (status %d)", len(batch), status)
	if q.onDrop != nil {
		q.onDrop(batch, status)
	}
}

// flush blocks until everything queued has been sent or dropped, or ctx ends.
func (q *queue) flush(ctx context.Context) error {
	r := make(chan struct{})
	select {
	case q.flushC <- r:
	case <-q.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-r:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// shutdown stops accepting events, flushes, then stops the goroutine (abandoning retries if ctx ends first).
func (q *queue) shutdown(ctx context.Context) error {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	err := q.flush(ctx)
	q.once.Do(func() { close(q.stop) })
	select {
	case <-q.done:
	case <-ctx.Done():
		if err == nil {
			err = ctx.Err()
		}
	}
	q.mu.Lock()
	q.dropped.Add(int64(len(q.items)))
	q.items = nil
	q.mu.Unlock()
	return err
}
