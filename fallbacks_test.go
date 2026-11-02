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

func fbClient(host string, fb map[string]any) *Client {
	return New("lc_pk_x", Config{Host: host, Fallbacks: fb, FlagTimeout: 500 * time.Millisecond, MaxRetries: 1, FlushInterval: time.Hour})
}

func TestFallbacksServedWhenFlagIsMissing(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"request_id": "r1", "flags": map[string]any{
			"served": map[string]any{"enabled": true, "value": true, "reason": "condition_match", "condition_index": 2},
		}})
	}))
	defer s.Close()
	ctx := context.Background()
	c := fbClient(s.URL, map[string]any{"gone": "safe", "served": false})
	defer c.Shutdown(ctx)

	d, ok := c.GetFeatureFlagDetails(ctx, "gone", "u", FlagOptions{})
	if !ok || d.Value != "safe" || d.Reason != ReasonFallback || d.ConditionIndex != -1 {
		t.Fatalf("missing flag: %+v %v", d, ok)
	}
	if v, ok := c.GetFeatureFlag(ctx, "gone", "u", FlagOptions{DefaultValue: "def"}); !ok || v != "safe" {
		t.Fatalf("fallback beats default: %v %v", v, ok)
	}
	d, _ = c.GetFeatureFlagDetails(ctx, "served", "u", FlagOptions{})
	if d.Value != true || d.Reason != "condition_match" || d.ConditionIndex != 2 || d.RequestID != "r1" || d.EvaluatedLocally {
		t.Fatalf("served flag: %+v", d)
	}
	if v, ok := c.GetFeatureFlag(ctx, "nofb", "u", FlagOptions{DefaultValue: "def"}); ok || v != "def" {
		t.Fatalf("no fallback keeps old behaviour: %v %v", v, ok)
	}
}

func TestFallbackOnNetworkFailureEmitsNoExposure(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }))
	defer s.Close()
	c := fbClient(s.URL, map[string]any{"f": "x"})
	defer c.Shutdown(context.Background())
	if v, ok := c.GetFeatureFlag(context.Background(), "f", "u", FlagOptions{}); !ok || v != "x" {
		t.Fatalf("got %v %v", v, ok)
	}
	tc := NewTestClientWithFlags(nil)
	tc.cfg.Fallbacks = map[string]any{"f": true}
	tc.GetFeatureFlag(context.Background(), "f", "u", FlagOptions{})
	if n := len(tc.Captured()); n != 0 {
		t.Fatalf("fallback must not emit exposure, got %d events", n)
	}
}

func TestOnFlagsChangeAndStale(t *testing.T) {
	var fail atomic.Bool
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() {
			w.WriteHeader(500)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"flags": []any{}})
	}))
	defer s.Close()
	c := New("lc_pk_x", Config{Host: s.URL, SecretKey: "lc_sk_x", DisableFlagStream: true, FlagPollInterval: time.Hour, FlushInterval: time.Hour})
	defer c.Shutdown(context.Background())
	<-c.Ready()
	var changed, stale atomic.Int32
	c.OnFlagsChange(func() { changed.Add(1) }, func(error) { stale.Add(1) })
	ctx := context.Background()
	_ = c.ReloadFeatureFlags(ctx)
	fail.Store(true)
	_ = c.ReloadFeatureFlags(ctx)
	if changed.Load() != 1 || stale.Load() != 1 {
		t.Fatalf("changed=%d stale=%d", changed.Load(), stale.Load())
	}
}
