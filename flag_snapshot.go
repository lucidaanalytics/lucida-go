package lucida

import (
	"context"
	"sort"
	"sync"
	"time"
)

// FlagSnapshot holds one distinct id's flags evaluated once (EvaluateFlags). Reads never call the
// network or emit exposure events; pass it to CaptureInput.Flags to attach its values to an event.
type FlagSnapshot struct {
	values, payloads map[string]any
	mu               sync.Mutex
	accessed         map[string]bool
}

// NewFlagSnapshot builds a snapshot from known values (tests, bootstrapping).
func NewFlagSnapshot(values, payloads map[string]any) *FlagSnapshot {
	return &FlagSnapshot{values: orEmpty(values), payloads: orEmpty(payloads), accessed: map[string]bool{}}
}

// GetFlag returns the value (false, true or a variant key) and records the key as accessed.
func (s *FlagSnapshot) GetFlag(key string) (any, bool) {
	s.mu.Lock()
	s.accessed[key] = true
	s.mu.Unlock()
	v, ok := s.values[key]
	return v, ok
}

// IsEnabled reports whether the flag is on (true or any variant).
func (s *FlagSnapshot) IsEnabled(key string) bool {
	v, ok := s.GetFlag(key)
	return ok && v != false
}

// GetPayload returns the payload of the flag's value, or nil.
func (s *FlagSnapshot) GetPayload(key string) any { return s.payloads[key] }

// Keys lists the snapshot's flag keys, sorted.
func (s *FlagSnapshot) Keys() []string {
	out := make([]string, 0, len(s.values))
	for k := range s.values {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Only returns a snapshot narrowed to keys.
func (s *FlagSnapshot) Only(keys ...string) *FlagSnapshot {
	v, p := map[string]any{}, map[string]any{}
	for _, k := range keys {
		if x, ok := s.values[k]; ok {
			v[k] = x
		}
		if x, ok := s.payloads[k]; ok {
			p[k] = x
		}
	}
	return NewFlagSnapshot(v, p)
}

// OnlyAccessed returns a snapshot of the flags read so far through GetFlag / IsEnabled.
func (s *FlagSnapshot) OnlyAccessed() *FlagSnapshot {
	s.mu.Lock()
	keys := make([]string, 0, len(s.accessed))
	for k := range s.accessed {
		keys = append(keys, k)
	}
	s.mu.Unlock()
	return s.Only(keys...)
}

// flagEventProps adds $feature/<key> and $active_feature_flags to props without overwriting them.
func flagEventProps(props, values map[string]any) map[string]any {
	if len(values) == 0 {
		return props
	}
	out := make(map[string]any, len(props)+len(values)+1)
	active := []string{}
	for k, v := range values {
		out["$feature/"+k] = v
		if v != false {
			active = append(active, k)
		}
	}
	sort.Strings(active)
	out["$active_feature_flags"] = active
	for k, v := range props {
		out[k] = v
	}
	return out
}

// EvaluateFlags evaluates every flag for distinctID once (locally where possible, one remote call
// for the rest) and returns the snapshot.
func (c *Client) EvaluateFlags(ctx context.Context, distinctID string, o FlagOptions) *FlagSnapshot {
	if c.cfg.Disabled {
		return NewFlagSnapshot(nil, nil)
	}
	v, p := c.allFlags(ctx, distinctID, o)
	return NewFlagSnapshot(v, p)
}

// withFlags resolves CaptureInput.Flags / SendFeatureFlags; done reports the event is handled
// asynchronously (a remote evaluation was needed).
func (c *Client) withFlags(in CaptureInput) (CaptureInput, bool) {
	if in.Flags != nil {
		in.Properties = flagEventProps(in.Properties, in.Flags.values)
		return in, false
	}
	if !in.SendFeatureFlags {
		return in, false
	}
	o := in.FlagOptions
	if o.Groups == nil {
		o.Groups = in.Groups
	}
	if loc := c.local(in.DistinctID, o); loc != nil {
		vals := map[string]any{}
		for k, r := range loc {
			if r.Reason != ReasonRequiresServer {
				vals[k] = r.Value
			}
		}
		in.Properties = flagEventProps(in.Properties, vals)
		return in, false
	}
	if in.Timestamp.IsZero() {
		in.Timestamp = time.Now()
	}
	// shortcut: queued after the flags request, so it may land behind later events (ordering is by timestamp)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), c.cfg.Timeout)
		defer cancel()
		vals, _ := c.allFlags(ctx, in.DistinctID, o)
		in.Properties, in.SendFeatureFlags = flagEventProps(in.Properties, vals), false
		c.Capture(in)
	}()
	return in, true
}
