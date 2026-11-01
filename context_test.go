package lucida

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRequestContextMergeNestingAndOverride(t *testing.T) {
	c := NewTestClientWithFlags(nil)
	ctx := WithRequestContext(context.Background(), RequestContext{DistinctID: "u1", SessionID: "s1", Properties: map[string]any{"a": 1, "b": 1}})
	child := WithRequestContext(ctx, RequestContext{Properties: map[string]any{"b": 2}})
	fresh := WithRequestContext(ctx, RequestContext{Fresh: true})
	c.CaptureContext(child, CaptureInput{Event: "inner"})
	c.CaptureContext(ctx, CaptureInput{DistinctID: "explicit", Event: "outer", Properties: map[string]any{"a": 9}})
	c.CaptureContext(fresh, CaptureInput{DistinctID: "x", Event: "fresh"})
	ev := c.Captured()
	if ev[0].DistinctID != "u1" || ev[0].Properties["$session_id"] != "s1" || ev[0].Properties["b"] != 2 || ev[0].Properties["a"] != 1 {
		t.Fatalf("%+v", ev[0])
	}
	if ev[1].DistinctID != "explicit" || ev[1].Properties["a"] != 9 || ev[1].Properties["b"] != 1 {
		t.Fatalf("%+v", ev[1])
	}
	if _, ok := ev[2].Properties["$session_id"]; ok {
		t.Fatal("fresh context must not inherit")
	}
	if rc, _ := RequestContextFrom(ctx); rc.Properties["b"] != 1 {
		t.Fatal("parent modified by child")
	}
}

func TestPersonlessAndMissingDistinctID(t *testing.T) {
	var gotErr error
	c := NewTestClientWithFlags(nil)
	c.cfg.OnError = func(err error, _ []CapturedEvent) { gotErr = err }
	c.Capture(CaptureInput{Event: "no-id"})
	if len(c.Captured()) != 0 || gotErr == nil {
		t.Fatalf("missing id must drop and report: %v", gotErr)
	}
	c.Capture(CaptureInput{Event: "p", Personless: true, Set: map[string]any{"a": 1}})
	e := c.Captured()[0]
	if e.Properties["$process_person_profile"] != false || len(e.DistinctID) != 36 || e.Set != nil {
		t.Fatalf("%+v", e)
	}
}

func TestTracingHeadersSanitised(t *testing.T) {
	h := http.Header{}
	h.Set("X-Lucida-Distinct-Id", " a\x00b"+strings.Repeat("z", 2000))
	h.Set("X-Lucida-Session-Id", "   ")
	d, s := ReadTracingHeaders(h)
	if len(d) != 1000 || !strings.HasPrefix(d, "ab") || s != "" {
		t.Fatalf("%d %q", len(d), s)
	}
}

func TestBeforeSendDropEditPanic(t *testing.T) {
	var errs []error
	c := NewTestClientWithFlags(nil)
	c.cfg.OnError = func(err error, _ []CapturedEvent) { errs = append(errs, err) }
	c.cfg.BeforeSend = func(e CapturedEvent) *CapturedEvent {
		switch e.Event {
		case "drop":
			return nil
		case "boom":
			e.Properties["half"] = true
			panic("x")
		}
		e.Properties["edited"] = true
		return &e
	}
	for _, n := range []string{"drop", "boom", "ok"} {
		c.Capture(CaptureInput{DistinctID: "u", Event: n})
	}
	ev := c.Captured()
	if len(ev) != 2 || ev[0].Event != "boom" || ev[1].Properties["edited"] != true || len(errs) != 1 {
		t.Fatalf("%+v %v", ev, errs)
	}
	if _, bad := ev[0].Properties["half"]; bad {
		t.Fatal("a panicking hook must not leave edits on the original")
	}
}

func TestGeoipDisable(t *testing.T) {
	c := NewTestClientWithFlags(nil)
	c.cfg.DisableGeoip = true
	off := false
	c.Capture(CaptureInput{DistinctID: "u", Event: "a"})
	c.Capture(CaptureInput{DistinctID: "u", Event: "b", DisableGeoip: &off})
	ev := c.Captured()
	if ev[0].Properties["$geoip_disable"] != true || ev[1].Properties["$geoip_disable"] != nil {
		t.Fatalf("%+v", ev)
	}
	c2 := NewTestClientWithFlags(nil)
	on := true
	c2.Capture(CaptureInput{DistinctID: "u", Event: "c", DisableGeoip: &on})
	if c2.Captured()[0].Properties["$geoip_disable"] != true {
		t.Fatal("per event")
	}
}

func TestMaxBatchSizeAndOnError(t *testing.T) {
	f := newFake(t)
	var mu sync.Mutex
	var drops []int
	f.respond = func(p string, n int) (int, map[string]string) {
		if p == "/v1/batch" {
			return 400, nil
		}
		return 0, nil
	}
	c := New("k", Config{Host: f.URL, FlushAt: 10, MaxBatchSize: 2, FlushInterval: time.Hour,
		OnError: func(err error, ev []CapturedEvent) {
			var de *DeliveryError
			if !errors.As(err, &de) || de.Status != 400 {
				t.Errorf("err %v", err)
			}
			mu.Lock()
			drops = append(drops, len(ev))
			mu.Unlock()
		}})
	for i := 0; i < 5; i++ {
		c.Capture(CaptureInput{DistinctID: "u", Event: "e"})
	}
	if err := c.Flush(ctx5()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(drops) != 3 || drops[0] != 2 || drops[2] != 1 || c.DroppedEvents() != 5 {
		t.Fatalf("drops=%v dropped=%d", drops, c.DroppedEvents())
	}
	_ = c.Shutdown(ctx5())
}

func TestFlagTimeoutAndRemoteConfig(t *testing.T) {
	f := newFake(t)
	f.remoteCfg = `{"key":"a/b","enabled":true,"payload":{"a":1}}`
	c := New("lc_pk", Config{Host: f.URL, SecretKey: "lc_sk", DisableFlagStream: true, FlagTimeout: 2 * time.Second, FlagDedupCacheSize: 5})
	<-c.Ready()
	p, ok, err := c.GetRemoteConfigPayload(ctx5(), "a/b")
	if err != nil || !ok || p.(map[string]any)["a"] != float64(1) {
		t.Fatalf("%v %v %v", p, ok, err)
	}
	reqs := f.to("/v1/flags/a/b/remote-config")
	if len(reqs) != 1 || reqs[0].escaped != "/v1/flags/a%2Fb/remote-config" || reqs[0].header.Get("Authorization") != "Bearer lc_sk" {
		t.Fatalf("%+v", reqs)
	}
	f.mu.Lock()
	f.remoteCfg = ""
	f.mu.Unlock()
	if p, ok, err := c.GetRemoteConfigPayload(ctx5(), "nope"); p != nil || ok || err != nil {
		t.Fatalf("404: %v %v %v", p, ok, err)
	}
	if _, _, err := New("k", Config{Host: f.URL, Disabled: true}).GetRemoteConfigPayload(ctx5(), "cfg"); err == nil {
		t.Fatal("needs secret key")
	}
	_ = c.Shutdown(ctx5())
}

func TestRequestContextMiddleware(t *testing.T) {
	var got RequestContext
	h := NewRequestContextMiddleware(nil, RequestContextOptions{ExtraProperties: map[string]any{"svc": "api"}})(
		http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { got, _ = RequestContextFrom(r.Context()) }))
	r := httptest.NewRequest("POST", "http://x.dev/p?a=1", nil)
	r.RemoteAddr = "9.9.9.9:1234"
	r.Header.Set("User-Agent", "UA")
	r.Header.Set("X-Lucida-Distinct-Id", "d1")
	r.Header.Set("X-Lucida-Session-Id", "s1")
	r.Header.Set("X-Forwarded-For", "1.2.3.4, 5.6.7.8")
	h.ServeHTTP(httptest.NewRecorder(), r)
	p := got.Properties
	if got.DistinctID != "d1" || got.SessionID != "s1" || p["$current_url"] != "http://x.dev/p?a=1" || p["$request_method"] != "POST" ||
		p["$request_path"] != "/p" || p["$user_agent"] != "UA" || p["$ip"] != "9.9.9.9" || p["svc"] != "api" {
		t.Fatalf("%+v", got)
	}
	h = NewRequestContextMiddleware(nil, RequestContextOptions{TrustProxy: true, NoTracingHeaders: true})(
		http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { got, _ = RequestContextFrom(r.Context()) }))
	h.ServeHTTP(httptest.NewRecorder(), r)
	if got.DistinctID != "" || got.Properties["$ip"] != "1.2.3.4" {
		t.Fatalf("%+v", got)
	}
	got = RequestContext{}
	h = NewRequestContextMiddleware(nil, RequestContextOptions{RequestFilter: func(*http.Request) bool { return false }})(
		http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			_, ok := RequestContextFrom(r.Context())
			got.DistinctID = map[bool]string{true: "has", false: ""}[ok]
		}))
	h.ServeHTTP(httptest.NewRecorder(), r)
	if got.DistinctID != "" {
		t.Fatal("filtered request must have no context")
	}
}
