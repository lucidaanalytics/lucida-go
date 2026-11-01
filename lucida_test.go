package lucida

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type recorded struct {
	path, query string
	escaped     string // r.URL.EscapedPath()
	header      http.Header
	body        map[string]any
}

type fake struct {
	*httptest.Server
	mu        sync.Mutex
	reqs      []recorded
	respond   func(path string, n int) (status int, hdr map[string]string) // n = nth call to path; status 0 = default
	flagDefs  string
	etag      string
	remoteOut string
	remoteCfg string // body of GET /v1/flags/{key}/remote-config; "" = 404
}

func newFake(t *testing.T) *fake {
	f := &fake{flagDefs: `{"flags":[]}`, etag: `"v1"`, remoteOut: `{"flags":{}}`}
	counts := map[string]int{}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rd io.Reader = r.Body
		if r.URL.Query().Get("compression") == "gzip" {
			zr, err := gzip.NewReader(r.Body)
			if err != nil {
				t.Errorf("bad gzip: %v", err)
				return
			}
			rd = zr
		}
		raw, _ := io.ReadAll(rd)
		rec := recorded{path: r.URL.Path, query: r.URL.RawQuery, escaped: r.URL.EscapedPath(), header: r.Header.Clone()}
		_ = json.Unmarshal(raw, &rec.body)
		f.mu.Lock()
		f.reqs = append(f.reqs, rec)
		counts[r.URL.Path]++
		n := counts[r.URL.Path]
		remoteCfg := f.remoteCfg
		f.mu.Unlock()
		if f.respond != nil {
			if st, hdr := f.respond(r.URL.Path, n); st != 0 {
				for k, v := range hdr {
					w.Header().Set(k, v)
				}
				w.WriteHeader(st)
				return
			}
		}
		if strings.HasSuffix(r.URL.Path, "/remote-config") {
			if remoteCfg == "" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = io.WriteString(w, remoteCfg)
			return
		}
		switch r.URL.Path {
		case "/v1/flags/definitions":
			if r.Header.Get("If-None-Match") == f.etag {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			w.Header().Set("ETag", f.etag)
			_, _ = io.WriteString(w, f.flagDefs)
		case "/v1/flags":
			_, _ = io.WriteString(w, f.remoteOut)
		default:
			_, _ = io.WriteString(w, `{"status":1}`)
		}
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *fake) to(path string) []recorded {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []recorded
	for _, r := range f.reqs {
		if r.path == path {
			out = append(out, r)
		}
	}
	return out
}

func (f *fake) bodies(path string) []map[string]any {
	var out []map[string]any
	for _, r := range f.to(path) {
		out = append(out, r.body)
	}
	return out
}

func (f *fake) events(path string) []map[string]any {
	var out []map[string]any
	for _, b := range f.bodies(path) {
		for _, e := range b["batch"].([]any) {
			out = append(out, e.(map[string]any))
		}
	}
	return out
}

func ctx5() context.Context {
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = cancel
	return c
}

const flagDefs = `{"flags":[
 {"key":"on","type":"boolean","active":true,"salt":"s","aggregation":{"kind":"person"},"conditions":[{"rollout_pct":100}]},
 {"key":"pro","type":"boolean","active":true,"salt":"s","aggregation":{"kind":"person"},"conditions":[{"rollout_pct":100,"filters":[{"type":"person","key":"plan","op":"eq","value":"pro"}]}]},
 {"key":"cohorted","type":"boolean","active":true,"salt":"s","aggregation":{"kind":"person"},"conditions":[{"rollout_pct":100,"filters":[{"type":"cohort","key":"c1","op":"in","value":1}]}]}]}`

func TestFlushAtBatchSizeAndInterval(t *testing.T) {
	f := newFake(t)
	c := New("k", Config{Host: f.URL, FlushAt: 3, FlushInterval: time.Hour})
	for i := 0; i < 3; i++ {
		c.Capture(CaptureInput{DistinctID: "u", Event: "e"})
	}
	if err := c.Flush(ctx5()); err != nil || len(f.events("/v1/batch")) != 3 {
		t.Fatalf("err=%v events=%d", err, len(f.events("/v1/batch")))
	}
	_ = c.Shutdown(ctx5())

	f2 := newFake(t)
	c2 := New("k", Config{Host: f2.URL, FlushInterval: 30 * time.Millisecond})
	c2.Capture(CaptureInput{DistinctID: "u", Event: "e"})
	for i := 0; i < 200 && len(f2.to("/v1/batch")) == 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if len(f2.events("/v1/batch")) != 1 {
		t.Fatal("interval flush did not happen")
	}
	_ = c2.Shutdown(ctx5())
}

func TestGzipAboveOneKB(t *testing.T) {
	f := newFake(t)
	c := New("k", Config{Host: f.URL})
	c.Capture(CaptureInput{DistinctID: "u", Event: "big", Properties: map[string]any{"blob": strings.Repeat("x", 3000)}})
	_ = c.Shutdown(ctx5())
	rs := f.to("/v1/batch")
	if len(rs) != 1 || rs[0].query != "compression=gzip" || len(f.events("/v1/batch")) != 1 {
		t.Fatalf("%+v", rs)
	}
}

func TestRetryHonoursRetryAfter(t *testing.T) {
	f := newFake(t)
	f.respond = func(p string, n int) (int, map[string]string) {
		if p == "/v1/batch" && n <= 2 {
			return []int{429, 503}[n-1], map[string]string{"Retry-After": "2"}
		}
		return 0, nil
	}
	c := New("k", Config{Host: f.URL})
	var ras []time.Duration
	c.queue.backoff = func(a int, ra time.Duration) time.Duration { ras = append(ras, ra); return time.Millisecond }
	c.Capture(CaptureInput{DistinctID: "u", Event: "e"})
	if err := c.Shutdown(ctx5()); err != nil {
		t.Fatal(err)
	}
	if len(f.to("/v1/batch")) != 3 || len(ras) != 2 || ras[0] != 2*time.Second || c.DroppedEvents() != 0 {
		t.Fatalf("reqs=%d ras=%v dropped=%d", len(f.to("/v1/batch")), ras, c.DroppedEvents())
	}
}

func TestBackoffJitterBounds(t *testing.T) {
	for i := 0; i < 100; i++ {
		if d := backoff(0, 0); d < 500*time.Millisecond || d > time.Second {
			t.Fatalf("%v", d)
		}
	}
	if backoff(0, 7*time.Second) != 7*time.Second || backoff(20, 0) > 30*time.Second {
		t.Fatal("retry-after floor or cap")
	}
}

func TestNonRetryableDropsAndCounts(t *testing.T) {
	f := newFake(t)
	f.respond = func(string, int) (int, map[string]string) { return 400, nil }
	c := New("k", Config{Host: f.URL})
	c.Capture(CaptureInput{DistinctID: "u", Event: "e"})
	_ = c.Flush(ctx5())
	if len(f.to("/v1/batch")) != 1 || c.DroppedEvents() != 1 {
		t.Fatalf("dropped=%d", c.DroppedEvents())
	}
	_ = c.Shutdown(ctx5())
}

func TestBoundedQueueDropsOldest(t *testing.T) {
	f := newFake(t)
	c := New("k", Config{Host: f.URL, MaxQueueSize: 3, FlushAt: 100, FlushInterval: time.Hour})
	for _, id := range []string{"u0", "u1", "u2", "u3", "u4"} {
		c.Capture(CaptureInput{DistinctID: id, Event: "e"})
	}
	if c.DroppedEvents() != 2 {
		t.Fatalf("dropped=%d", c.DroppedEvents())
	}
	_ = c.Shutdown(ctx5())
	var ids []string
	for _, e := range f.events("/v1/batch") {
		ids = append(ids, e["distinct_id"].(string))
	}
	if strings.Join(ids, ",") != "u2,u3,u4" {
		t.Fatalf("%v", ids)
	}
}

func TestDisabledAndShutdownBehaviour(t *testing.T) {
	f := newFake(t)
	d := New("k", Config{Host: f.URL, Disabled: true, SecretKey: "lc_sk"})
	d.Capture(CaptureInput{DistinctID: "u", Event: "e"})
	if _, ok := d.GetFeatureFlag(ctx5(), "on", "u", FlagOptions{}); ok || len(d.GetAllFlags(ctx5(), "u", FlagOptions{})) != 0 {
		t.Fatal("disabled flags")
	}
	if n, err := d.ImportEvents(ctx5(), nil); n != 0 || err != nil {
		t.Fatal(n, err)
	}
	_ = d.Shutdown(ctx5())
	c := New("k", Config{Host: f.URL})
	c.Capture(CaptureInput{DistinctID: "u", Event: "first"})
	_ = c.Shutdown(ctx5())
	c.Capture(CaptureInput{DistinctID: "u", Event: "late"})
	evs := f.events("/v1/batch")
	if len(f.reqs) != 1 || len(evs) != 1 || evs[0]["event"] != "first" {
		t.Fatalf("%v", f.reqs)
	}
}

func TestLocalFlagsEtagAndExposureDedupe(t *testing.T) {
	f := newFake(t)
	f.flagDefs = flagDefs
	c := New("lc_pk", Config{Host: f.URL, SecretKey: "lc_sk", FlagPollInterval: time.Hour})
	<-c.Ready()
	pro := FlagOptions{PersonProperties: map[string]any{"plan": "pro"}}
	free := FlagOptions{PersonProperties: map[string]any{"plan": "free"}}
	for _, tc := range []struct {
		o    FlagOptions
		want bool
	}{{pro, true}, {pro, true}, {free, false}} {
		if v, ok := c.GetFeatureFlag(ctx5(), "pro", "u1", tc.o); !ok || v != tc.want {
			t.Fatalf("v=%v ok=%v", v, ok)
		}
	}
	if on, ok := c.IsFeatureEnabled(ctx5(), "on", "u1", FlagOptions{}); !on || !ok {
		t.Fatal("on")
	}
	_ = c.Flush(ctx5())
	var got []string
	for _, e := range f.events("/v1/batch") {
		got = append(got, e["event"].(string)+"="+strings.TrimSpace(strings.ToLower(jsonOf(e["properties"].(map[string]any)["$feature_flag_response"]))))
	}
	want := "$feature_flag_called=true,$exposure=true,$feature_flag_called=false,$exposure=false,$feature_flag_called=true,$exposure=true"
	if strings.Join(got, ",") != want || len(f.to("/v1/flags")) != 0 {
		t.Fatalf("got %v", got)
	}
	if h := f.to("/v1/flags/definitions")[0].header.Get("Authorization"); h != "Bearer lc_sk" {
		t.Fatal(h)
	}
	if err := c.ReloadFeatureFlags(ctx5()); err != nil {
		t.Fatal(err)
	}
	if last := f.to("/v1/flags/definitions"); last[len(last)-1].header.Get("If-None-Match") != `"v1"` {
		t.Fatal("no If-None-Match")
	}
	f.flagDefs, f.etag = `{"flags":[]}`, `"v2"`
	_ = c.ReloadFeatureFlags(ctx5())
	if len(c.GetAllFlags(ctx5(), "u1", FlagOptions{OnlyEvaluateLocally: true})) != 0 {
		t.Fatal("definitions not replaced")
	}
	_ = c.Shutdown(ctx5())
}

func jsonOf(v any) string { b, _ := json.Marshal(v); return string(b) }

func TestRemoteFallback(t *testing.T) {
	f := newFake(t)
	f.flagDefs = flagDefs
	f.remoteOut = `{"request_id":"r1","flags":{"cohorted":{"enabled":true,"value":true},"other":{"enabled":true,"variant":"b","value":"b","payload":{"k":1}}}}`
	c := New("lc_pk", Config{Host: f.URL, SecretKey: "lc_sk", FlagPollInterval: time.Hour})
	<-c.Ready()
	o := FlagOptions{NoExposure: true}
	if v, ok := c.GetFeatureFlag(ctx5(), "cohorted", "u1", o); !ok || v != true {
		t.Fatal("cohorted", v, ok)
	}
	if v, _ := c.GetFeatureFlag(ctx5(), "other", "u1", o); v != "b" {
		t.Fatal("other", v)
	}
	if p, _ := c.GetFeatureFlagPayload(ctx5(), "other", "u1", o).(map[string]any); p["k"] != 1.0 {
		t.Fatal("payload", p)
	}
	if _, ok := c.GetFeatureFlag(ctx5(), "cohorted", "u1", FlagOptions{OnlyEvaluateLocally: true}); ok {
		t.Fatal("onlyEvaluateLocally must not call the server")
	}
	b := f.bodies("/v1/flags")[0]
	if b["api_key"] != "lc_pk" || b["distinct_id"] != "u1" || b["flag_keys"].([]any)[0] != "cohorted" {
		t.Fatalf("%v", b)
	}
	all := c.GetAllFlags(ctx5(), "u1", FlagOptions{PersonProperties: map[string]any{"plan": "pro"}})
	if len(all) != 4 || all["pro"] != true || all["other"] != "b" || all["cohorted"] != true {
		t.Fatalf("%v", all)
	}
	// remote-resolved exposure carries the request id and locally_evaluated=false
	c.GetFeatureFlag(ctx5(), "other", "u2", FlagOptions{})
	_ = c.Shutdown(ctx5())
	p := f.events("/v1/batch")[0]["properties"].(map[string]any)
	if p["locally_evaluated"] != false || p["$feature_flag_request_id"] != "r1" {
		t.Fatalf("%v", p)
	}
}

func TestImportEvents(t *testing.T) {
	f := newFake(t)
	c := New("k", Config{Host: f.URL, SecretKey: "lc_sk"})
	evs := make([]ImportEvent, 1500)
	for i := range evs {
		evs[i] = ImportEvent{DistinctID: "u", Event: "old", Timestamp: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)}
	}
	n, err := c.ImportEvents(ctx5(), evs)
	rs := f.to("/v1/s2s/batch")
	if err != nil || n != 1500 || len(rs) != 2 || len(rs[0].body["batch"].([]any)) != 1000 || len(rs[1].body["batch"].([]any)) != 500 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if rs[0].header.Get("Authorization") != "Bearer lc_sk" || rs[0].body["api_key"] != nil {
		t.Fatal("auth")
	}
	if ts := f.events("/v1/s2s/batch")[0]["timestamp"]; ts != "2020-01-01T00:00:00.000Z" {
		t.Fatal(ts)
	}
	f.respond = func(string, int) (int, map[string]string) { return 400, nil }
	if _, err := c.ImportEvents(ctx5(), evs[:1]); err == nil {
		t.Fatal("want error")
	}
	if _, err := New("k", Config{Host: f.URL}).ImportEvents(ctx5(), evs[:1]); err == nil {
		t.Fatal("want secret key error")
	}
	_ = c.Shutdown(ctx5())
}

func TestLRUSetEvictsLeastRecent(t *testing.T) {
	s := newLRUSet(2)
	if !s.add("a") || !s.add("b") || s.add("a") || !s.add("c") || s.len() != 2 || !s.add("b") {
		t.Fatal("lru")
	}
}
