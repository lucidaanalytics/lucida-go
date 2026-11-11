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
	keys  map[string]string // event uuid -> dedupe key, until the batch is dropped
}

// claim reports whether this exact call (kind plus id and property values) is new to the process and, if so,
// the uuid to send its event under. release frees the call again when that event's batch is dropped.
func (s *identityState) claim(parts ...any) (uuid string, ok bool) {
	s.once.Do(func() { s.seen = newLRUSet(identityDedupeSize); s.keys = map[string]string{} })
	uuid = uuidV7()
	raw, err := json.Marshal(parts)
	if err != nil {
		return uuid, true // unhashable properties: never suppress
	}
	sum := sha256.Sum256(raw)
	key := hex.EncodeToString(sum[:])
	if !s.seen.add(key) {
		return "", false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.keys) >= identityDedupeSize {
		for k := range s.keys { // bounded like the set; an evicted uuid just stays deduped
			delete(s.keys, k)
			break
		}
	}
	s.keys[uuid] = key
	return uuid, true
}

// release forgets the calls whose events were in a dropped batch, so repeating them is sent again.
func (s *identityState) release(batch []wireEvent) {
	s.once.Do(func() { s.seen = newLRUSet(identityDedupeSize); s.keys = map[string]string{} })
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range batch {
		if key, ok := s.keys[e.UUID]; ok {
			delete(s.keys, e.UUID)
			s.seen.remove(key)
		}
	}
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
