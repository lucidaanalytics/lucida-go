package lucida

import (
	"bufio"
	"context"
	"errors"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"
)

// streamLoop consumes GET /v1/flags/stream (SSE `definitions` events): each event reloads the
// definitions (ETag aware). It reconnects with exponential backoff and jitter; while it is down
// c.streaming is false and poll() resumes interval reloads.
func (c *Client) streamLoop() {
	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-c.stopPoll; cancel() }()
	base := c.cfg.StreamBackoffBase
	if base <= 0 {
		base = time.Second
	}
	for attempt := 0; ctx.Err() == nil; attempt++ {
		if c.streamOnce(ctx) {
			attempt = 0
		}
		c.streaming.Store(false)
		if ctx.Err() == nil {
			c.fireStale(errors.New("lucida: flag stream down"))
		}
		d := min(base<<min(attempt, 6), 30*time.Second)
		d = d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return
		}
	}
}

// streamOnce runs one connection; it reports whether the stream was established.
func (c *Client) streamOnce(ctx context.Context) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.host+"/v1/flags/stream", nil)
	if err != nil {
		return false
	}
	req.Header.Set("Authorization", "Bearer "+c.secret)
	req.Header.Set("Accept", "text/event-stream")
	resp, err := c.http.Do(req)
	if err != nil {
		c.logf("flag stream failed: %v", err)
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		c.logf("flag stream: status %d", resp.StatusCode)
		return false
	}
	c.streaming.Store(true)
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 4096), 1<<20)
	event := ""
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if event == "definitions" {
				_ = c.ReloadFeatureFlags(ctx)
				c.readyOne.Do(func() { close(c.ready) })
			}
			event = ""
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(line[len("event:"):])
		}
	}
	if err := sc.Err(); err != nil && ctx.Err() == nil {
		c.logf("flag stream read: %v", err)
	}
	return true
}
