package lucida

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// sseGateway is a fake gateway: /v1/flags/stream pushes on demand, /v1/flags/definitions serves `on`.
type sseGateway struct {
	*httptest.Server
	on      atomic.Bool
	streams atomic.Int32
	loads   atomic.Int32
	failing atomic.Bool
	mu      sync.Mutex
	push    chan struct{}
	drop    chan struct{}
}

func newSSEGateway() *sseGateway {
	g := &sseGateway{push: make(chan struct{}, 4), drop: make(chan struct{}, 1)}
	g.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/flags/stream":
			g.streams.Add(1)
			if g.failing.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "text/event-stream")
			fl := w.(http.Flusher)
			fmt.Fprint(w, "event: definitions\ndata: {\"etag\":\"v0\"}\n\n")
			fl.Flush()
			for {
				select {
				case <-g.push:
					fmt.Fprint(w, ": keepalive\n\nevent: definitions\ndata: {\"etag\":\"v\"}\n\n")
					fl.Flush()
				case <-g.drop:
					return
				case <-r.Context().Done():
					return
				}
			}
		case "/v1/flags/definitions":
			g.loads.Add(1)
			fmt.Fprintf(w, `{"flags":[{"key":"f","type":"boolean","active":%t,"salt":"s","aggregation":{"kind":"person"},"conditions":[{"rollout_pct":100}]}]}`, g.on.Load())
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	return g
}

func waitFor(t *testing.T, what string, f func() bool) {
	t.Helper()
	for end := time.Now().Add(3 * time.Second); time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
		if f() {
			return
		}
	}
	t.Fatalf("timeout waiting for %s", what)
}

func newStreamClient(g *sseGateway, poll time.Duration) *Client {
	return New("lc_pk_x", Config{Host: g.URL, SecretKey: "lc_sk_x", FlagPollInterval: poll, StreamBackoffBase: 10 * time.Millisecond})
}

func enabled(c *Client) bool {
	v, _ := c.IsFeatureEnabled(ctx5(), "f", "u1", FlagOptions{OnlyEvaluateLocally: true})
	return v
}

func TestStreamPushReloadsDefinitions(t *testing.T) {
	g := newSSEGateway()
	defer g.Close()
	c := newStreamClient(g, time.Minute)
	defer c.Shutdown(ctx5())
	<-c.Ready()
	if enabled(c) {
		t.Fatal("flag should start off")
	}
	g.on.Store(true)
	g.push <- struct{}{}
	waitFor(t, "flag flip via push", func() bool { return enabled(c) })
}

func TestStreamReconnectsAndFallsBackToPolling(t *testing.T) {
	g := newSSEGateway()
	defer g.Close()
	c := newStreamClient(g, 20*time.Millisecond)
	defer c.Shutdown(ctx5())
	<-c.Ready()
	waitFor(t, "first stream", func() bool { return g.streams.Load() == 1 })
	g.failing.Store(true)
	g.drop <- struct{}{}
	waitFor(t, "reconnect attempts", func() bool { return g.streams.Load() >= 3 })
	g.on.Store(true) // no push possible: only polling can see this
	waitFor(t, "polling fallback", func() bool { return enabled(c) })
}
