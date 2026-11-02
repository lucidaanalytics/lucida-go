package lucidaprovider

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/open-feature/go-sdk/openfeature"

	lucida "github.com/lucidaanalytics/lucida-go"
)

type fakeFlags struct {
	details  map[string]lucida.FlagDetails
	captured []lucida.CaptureInput
	gotOpts  lucida.FlagOptions
	changed  func()
	stale    func(error)
}

func (f *fakeFlags) GetFeatureFlagDetails(_ context.Context, key, _ string, o lucida.FlagOptions) (lucida.FlagDetails, bool) {
	f.gotOpts = o
	d, ok := f.details[key]
	return d, ok
}
func (f *fakeFlags) Capture(in lucida.CaptureInput) { f.captured = append(f.captured, in) }
func (f *fakeFlags) OnFlagsChange(c func(), s func(error)) {
	f.changed, f.stale = c, s
}

var fc = openfeature.FlattenedContext{openfeature.TargetingKey: "u1"}

// testdata is $LUCIDA_TESTDATA/rel, else the nearest testdata/rel above the working dir (monorepo and mirror both work).
func testdata(t *testing.T, rel string) string {
	if d := os.Getenv("LUCIDA_TESTDATA"); d != "" {
		return filepath.Join(d, rel)
	}
	dir, _ := os.Getwd()
	for {
		if p := filepath.Join(dir, "testdata", rel); fileExists(p) {
			return p
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("testdata/%s not found", rel)
		}
		dir = parent
	}
}

func fileExists(p string) bool { _, err := os.Stat(p); return err == nil }

func TestReasonsMatchSharedTable(t *testing.T) {
	raw, err := os.ReadFile(testdata(t, "openfeature/reasons.json"))
	if err != nil {
		t.Fatal(err)
	}
	var table struct {
		Reasons map[string]struct {
			Reason        string `json:"reason"`
			VariantReason string `json:"variant_reason"`
		} `json:"reasons"`
	}
	if err := json.Unmarshal(raw, &table); err != nil {
		t.Fatal(err)
	}
	for k, want := range table.Reasons {
		if got := ofReason(k, true); string(got) != want.Reason {
			t.Errorf("%q: got %s want %s", k, got, want.Reason)
		}
		if want.VariantReason != "" {
			if got := ofReason(k, "v"); string(got) != want.VariantReason {
				t.Errorf("%q variant: got %s want %s", k, got, want.VariantReason)
			}
		}
	}
	if ofReason("never_heard_of_it", true) != openfeature.UnknownReason {
		t.Error("unknown reason must map to UNKNOWN")
	}
}

func TestFallbackAndMetadata(t *testing.T) {
	f := &fakeFlags{details: map[string]lucida.FlagDetails{
		"fb":  {Key: "fb", Value: "safe", Reason: lucida.ReasonFallback, ConditionIndex: -1},
		"cfg": {Key: "cfg", Value: true, Payload: 3.0, Reason: "condition_match", ConditionIndex: 1, RequestID: "r9"},
	}}
	p := New(f)
	s := p.StringEvaluation(context.Background(), "fb", "def", fc)
	if s.Value != "safe" || s.Reason != openfeature.DefaultReason || s.FlagMetadata["lucidaReason"] != "fallback" {
		t.Fatalf("fallback: %+v", s)
	}
	if _, has := s.FlagMetadata["conditionIndex"]; has {
		t.Fatal("conditionIndex must be absent when < 0")
	}
	n := p.IntEvaluation(context.Background(), "cfg", 0, fc)
	m := n.FlagMetadata
	if n.Value != 3 || n.Reason != openfeature.TargetingMatchReason || m["conditionIndex"] != 1 || m["requestId"] != "r9" || m["evaluatedLocally"] != false {
		t.Fatalf("served: %+v", n)
	}
	if b := p.BooleanEvaluation(context.Background(), "gone", true, fc); b.Reason != openfeature.ErrorReason || !b.Value {
		t.Fatalf("missing flag must error: %+v", b)
	}
	if b := p.BooleanEvaluation(context.Background(), "cfg", false, openfeature.FlattenedContext{}); b.Reason != openfeature.ErrorReason {
		t.Fatalf("missing targeting key: %+v", b)
	}
}

func TestContextGroupsAndTrack(t *testing.T) {
	f := &fakeFlags{details: map[string]lucida.FlagDetails{"x": {Value: true, Reason: "condition_match", ConditionIndex: 0}}}
	p := New(f)
	ctx := openfeature.FlattenedContext{openfeature.TargetingKey: "u1", "plan": "pro",
		"groups":          map[string]any{"company": "acme"},
		"groupProperties": map[string]any{"company": map[string]any{"size": 10}}}
	p.BooleanEvaluation(context.Background(), "x", false, ctx)
	o := f.gotOpts
	if o.Groups["company"] != "acme" || o.GroupProperties["company"]["size"] != 10 || o.PersonProperties["plan"] != "pro" || len(o.PersonProperties) != 1 {
		t.Fatalf("opts: %+v", o)
	}
	ec := openfeature.NewEvaluationContext("u1", map[string]any{"groups": map[string]any{"company": "acme"}})
	p.Track(context.Background(), "purchase", ec, openfeature.NewTrackingEventDetails(9.5).Add("sku", "a"))
	if len(f.captured) != 1 {
		t.Fatalf("captured %d", len(f.captured))
	}
	c := f.captured[0]
	if c.Event != "purchase" || c.DistinctID != "u1" || c.Properties["$value"] != 9.5 || c.Properties["sku"] != "a" || c.Groups["company"] != "acme" {
		t.Fatalf("track: %+v", c)
	}
}

func next(t *testing.T, p *Provider) openfeature.EventType {
	t.Helper()
	select {
	case e := <-p.EventChannel():
		return e.EventType
	case <-time.After(time.Second):
		t.Fatal("no event")
		return ""
	}
}

func TestProviderEvents(t *testing.T) {
	f := &fakeFlags{}
	p := New(f)
	f.stale(errors.New("boom")) // first load failed
	if next(t, p) != openfeature.ProviderError || p.Init(openfeature.EvaluationContext{}) == nil {
		t.Fatal("first failure must be ERROR and fail Init")
	}
	f.changed() // first successful load: announced by Init, no event
	f.changed()
	if got := next(t, p); got != openfeature.ProviderConfigChange {
		t.Fatalf("got %s", got)
	}
	f.stale(errors.New("down"))
	if got := next(t, p); got != openfeature.ProviderStale {
		t.Fatalf("got %s", got)
	}
	f.changed() // recovery
	if got := next(t, p); got != openfeature.ProviderReady {
		t.Fatalf("got %s", got)
	}
}
