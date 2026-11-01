package lucida

import "sync"

// StickyStore persists the value each unit got from a sticky flag (LC-EXT-29), keyed by the flag's
// bucketing key, so the unit keeps it when rollout or variant weights change. Set Config.StickyStore.
type StickyStore interface {
	Get(bucketingKey string) map[string]any
	Set(bucketingKey, flagKey string, value any)
}

// MemoryStickyStore is an in-process StickyStore (lost on restart), safe for concurrent use.
type MemoryStickyStore struct {
	mu sync.Mutex
	m  map[string]map[string]any
}

// NewMemoryStickyStore returns an empty in-memory store.
func NewMemoryStickyStore() *MemoryStickyStore {
	return &MemoryStickyStore{m: map[string]map[string]any{}}
}

// Get returns a copy of the stored assignments of a unit (flag key -> value).
func (s *MemoryStickyStore) Get(key string) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]any, len(s.m[key]))
	for k, v := range s.m[key] {
		out[k] = v
	}
	return out
}

// Set stores value for (flag, unit).
func (s *MemoryStickyStore) Set(key, flag string, value any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m[key] == nil {
		s.m[key] = map[string]any{}
	}
	s.m[key][flag] = value
}

// EvaluateAllSticky is EvaluateAll with stored assignments loaded from store and fresh sticky
// matches written back.
func EvaluateAllSticky(flags []Flag, c EvalContext, store StickyStore) map[string]FlagResult {
	assigned := make(map[string]any, len(c.StickyAssignments))
	for k, v := range c.StickyAssignments {
		assigned[k] = v
	}
	for _, f := range flags {
		if k := bucketingKey(f, c); f.Sticky && k != "" {
			if v, ok := store.Get(k)[f.Key]; ok {
				assigned[f.Key] = v
			}
		}
	}
	c.StickyAssignments = assigned
	out := EvaluateAll(flags, c)
	for _, f := range flags {
		if v, ok := StickyValue(f, out[f.Key]); ok {
			if k := bucketingKey(f, c); k != "" {
				store.Set(k, f.Key, v)
			}
		}
	}
	return out
}
