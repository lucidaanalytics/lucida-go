package lucida

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func defsServer(t *testing.T, loads *atomic.Int32) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/flags/definitions" {
			loads.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"flags": snapFlags()})
			return
		}
		_, _ = w.Write([]byte("{}"))
	}))
	t.Cleanup(s.Close)
	return s
}

func cacheClient(host string, p FlagDefinitionCache) *Client {
	return New("lc_pk_x", Config{Host: host, SecretKey: "lc_sk_x", DisableFlagStream: true, FlagPollInterval: time.Hour, FlushInterval: time.Hour, FlagDefinitionCache: p})
}

func TestFlagDefinitionCacheAdoptedAndPublished(t *testing.T) {
	var loads atomic.Int32
	s := defsServer(t, &loads)
	ctx := context.Background()

	warm := &InMemoryFlagDefinitionCache{TTL: time.Minute}
	warm.OnFlagDefinitionsReceived(ctx, snapFlags())
	c := cacheClient(s.URL, warm)
	if err := c.ReloadFeatureFlags(ctx); err != nil || loads.Load() != 0 {
		t.Fatalf("fresh cache must not fetch: err=%v loads=%d", err, loads.Load())
	}
	if v, ok := c.GetFeatureFlag(ctx, "a", "u", FlagOptions{OnlyEvaluateLocally: true}); !ok || v != true {
		t.Fatalf("adopted definitions: %v", v)
	}
	_ = c.Shutdown(ctx)

	empty := &InMemoryFlagDefinitionCache{}
	c2 := cacheClient(s.URL, empty)
	if err := c2.ReloadFeatureFlags(ctx); err != nil || loads.Load() < 1 {
		t.Fatalf("empty cache must fetch: err=%v loads=%d", err, loads.Load())
	}
	if defs, ok := empty.FlagDefinitions(ctx); !ok || len(defs) != 4 {
		t.Fatalf("published: %v %v", defs, ok)
	}
	_ = c2.Shutdown(ctx)
}

type brokenCache struct{}

func (brokenCache) FlagDefinitions(context.Context) ([]Flag, bool)  { panic("down") }
func (brokenCache) ShouldFetchFlagDefinitions(context.Context) bool { panic("down") }
func (brokenCache) OnFlagDefinitionsReceived(context.Context, []Flag) {
	panic("down")
}
func (brokenCache) Shutdown() { panic("down") }

func TestBrokenFlagDefinitionCacheMeansFetch(t *testing.T) {
	var loads atomic.Int32
	s := defsServer(t, &loads)
	c := cacheClient(s.URL, brokenCache{})
	if err := c.ReloadFeatureFlags(context.Background()); err != nil || loads.Load() < 1 {
		t.Fatalf("err=%v loads=%d", err, loads.Load())
	}
	_ = c.Shutdown(context.Background())
}
