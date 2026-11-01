package lucida

import (
	"container/list"
	"crypto/rand"
	"encoding/hex"
	rand2 "math/rand/v2"
	"sync"
	"time"
)

// uuidV7 returns a time-ordered UUID.
func uuidV7() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	ms := uint64(time.Now().UnixMilli())
	for i := 0; i < 6; i++ {
		b[i] = byte(ms >> (8 * (5 - i)))
	}
	b[6] = 0x70 | b[6]&0x0f
	b[8] = 0x80 | b[8]&0x3f
	h := hex.EncodeToString(b[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// lruSet is a bounded set: adding past max evicts the least recently seen key.
type lruSet struct {
	mu  sync.Mutex
	max int
	l   *list.List
	m   map[string]*list.Element
}

func newLRUSet(max int) *lruSet {
	return &lruSet{max: max, l: list.New(), m: map[string]*list.Element{}}
}

// add reports whether key was new.
func (s *lruSet) add(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.m[key]; ok {
		s.l.MoveToFront(e)
		return false
	}
	s.m[key] = s.l.PushFront(key)
	if s.l.Len() > s.max {
		old := s.l.Back()
		s.l.Remove(old)
		delete(s.m, old.Value.(string))
	}
	return true
}

func (s *lruSet) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.l.Len()
}

func retryable(status int) bool {
	return status == 0 || status == 408 || status == 429 || status >= 500
}

// backoff is exponential (1 s base, 30 s cap) with 50%..100% jitter; a longer Retry-After wins.
func backoff(attempt int, retryAfter time.Duration) time.Duration {
	d := min(30*time.Second, time.Second<<min(attempt, 10))
	d = time.Duration(float64(d) * (0.5 + 0.5*rand2.Float64()))
	return max(d, retryAfter)
}
