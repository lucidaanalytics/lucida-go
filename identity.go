package lucida

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"
)

const identityDedupeSize = 1000

// identityState holds super properties and the bounded set of identify/group calls already sent.
type identityState struct {
	mu    sync.RWMutex
	super map[string]any
	once  sync.Once
	seen  *lruSet
}

// first reports whether this exact call (kind plus id and property values) is new to the process.
func (s *identityState) first(parts ...any) bool {
	s.once.Do(func() { s.seen = newLRUSet(identityDedupeSize) })
	raw, err := json.Marshal(parts)
	if err != nil {
		return true // unhashable properties: never suppress
	}
	sum := sha256.Sum256(raw)
	return s.seen.add(hex.EncodeToString(sum[:]))
}

func (s *identityState) copySuper(dst map[string]any) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for k, v := range s.super {
		dst[k] = v
	}
}

// Register adds super properties to every event; context and per-event properties win.
func (c *Client) Register(props map[string]any) {
	c.identity.mu.Lock()
	defer c.identity.mu.Unlock()
	if c.identity.super == nil {
		c.identity.super = map[string]any{}
	}
	for k, v := range props {
		c.identity.super[k] = v
	}
}

// Unregister removes a super property.
func (c *Client) Unregister(key string) {
	c.identity.mu.Lock()
	defer c.identity.mu.Unlock()
	delete(c.identity.super, key)
}

// SetPersonProperties sets person properties with a $set event (not deduped).
func (c *Client) SetPersonProperties(distinctID string, props map[string]any) {
	c.Capture(CaptureInput{DistinctID: distinctID, Event: "$set", Set: props})
}

// SetPersonPropertiesOnce sets person properties only where unset ($set_once).
func (c *Client) SetPersonPropertiesOnce(distinctID string, props map[string]any) {
	c.Capture(CaptureInput{DistinctID: distinctID, Event: "$set", SetOnce: props})
}
