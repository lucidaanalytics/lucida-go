package lucida

import (
	"context"
	"sync"
	"time"
)

// FlagDefinitionCache lets many workers share one definitions fetch (Redis, a database, files...).
// Before each reload the client asks ShouldFetchFlagDefinitions: when false it adopts FlagDefinitions instead of
// calling the server; the worker that did fetch calls OnFlagDefinitionsReceived. A provider that panics is
// treated as "fetch yourself". Set Config.FlagDefinitionCache.
type FlagDefinitionCache interface {
	// FlagDefinitions returns the cached definitions; ok is false when nothing is cached.
	FlagDefinitions(ctx context.Context) (defs []Flag, ok bool)
	// ShouldFetchFlagDefinitions is true when this process should call the server (for example it won the fetch lock).
	ShouldFetchFlagDefinitions(ctx context.Context) bool
	// OnFlagDefinitionsReceived stores freshly fetched definitions for the other workers.
	OnFlagDefinitionsReceived(ctx context.Context, defs []Flag)
	// Shutdown releases locks and connections.
	Shutdown()
}

// InMemoryFlagDefinitionCache is the reference provider: process-local, fetches again once TTL has passed.
type InMemoryFlagDefinitionCache struct {
	TTL time.Duration // default 30s

	mu   sync.Mutex
	defs []Flag
	have bool
	at   time.Time
}

func (m *InMemoryFlagDefinitionCache) ttl() time.Duration {
	if m.TTL > 0 {
		return m.TTL
	}
	return 30 * time.Second
}

func (m *InMemoryFlagDefinitionCache) FlagDefinitions(context.Context) ([]Flag, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.defs, m.have
}

func (m *InMemoryFlagDefinitionCache) ShouldFetchFlagDefinitions(context.Context) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return !m.have || time.Since(m.at) >= m.ttl()
}

func (m *InMemoryFlagDefinitionCache) OnFlagDefinitionsReceived(_ context.Context, defs []Flag) {
	m.mu.Lock()
	m.defs, m.have, m.at = defs, true, time.Now()
	m.mu.Unlock()
}

func (m *InMemoryFlagDefinitionCache) Shutdown() {}

// adoptCachedFlags returns the definitions to use instead of fetching, or ok=false when this process must fetch.
func adoptCachedFlags(ctx context.Context, p FlagDefinitionCache) (defs []Flag, ok bool) {
	if p == nil {
		return nil, false
	}
	defer func() {
		if recover() != nil {
			defs, ok = nil, false
		}
	}()
	if p.ShouldFetchFlagDefinitions(ctx) {
		return nil, false
	}
	return p.FlagDefinitions(ctx)
}

func publishCachedFlags(ctx context.Context, p FlagDefinitionCache, defs []Flag) {
	if p == nil {
		return
	}
	defer func() { _ = recover() }() // a failing cache must not break flag evaluation
	p.OnFlagDefinitionsReceived(ctx, defs)
}
