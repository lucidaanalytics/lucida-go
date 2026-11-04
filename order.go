package lucida

import (
	"context"
	"sync"
)

// orderGate keeps captured events in call order while one of them waits for a remote flag evaluation:
// each later event queues behind the tail instead of overtaking it. Idle, it costs a mutex and no goroutine.
type orderGate struct {
	mu   sync.Mutex
	tail chan struct{} // closed once every event entered so far has been emitted
}

// enter registers an event. With nothing pending and force false it returns nil, nil (emit inline).
// Otherwise prev (may be nil) must be received before emitting, and leave(mine) called after.
func (g *orderGate) enter(force bool) (prev, mine chan struct{}) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.tail == nil && !force {
		return nil, nil
	}
	prev, mine = g.tail, make(chan struct{})
	g.tail = mine
	return prev, mine
}

func (g *orderGate) leave(mine chan struct{}) {
	close(mine)
	g.mu.Lock()
	if g.tail == mine {
		g.tail = nil
	}
	g.mu.Unlock()
}

// wait blocks until every event entered so far has been emitted, or ctx ends.
func (g *orderGate) wait(ctx context.Context) error {
	g.mu.Lock()
	t := g.tail
	g.mu.Unlock()
	if t == nil {
		return nil
	}
	select {
	case <-t:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
