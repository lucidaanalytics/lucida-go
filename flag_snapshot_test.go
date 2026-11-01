package lucida

import (
	"context"
	"reflect"
	"testing"
)

func snapFlags() []Flag {
	on := func(key string, ctxs ...string) Flag {
		return Flag{Key: key, Type: TypeBoolean, Active: true, Salt: "s", Aggregation: Aggregation{Kind: "person"},
			AppContexts: ctxs, Conditions: []Condition{{RolloutPct: 100}}}
	}
	off := on("off")
	off.Conditions[0].RolloutPct = 0
	return []Flag{on("a"), on("b"), off, on("web", "web")}
}

func TestCaptureSendFeatureFlagsAndSnapshot(t *testing.T) {
	c := NewTestClientWithFlags(snapFlags())
	ctx := context.Background()
	c.Capture(CaptureInput{DistinctID: "u", Event: "buy", Properties: map[string]any{"$feature/a": "mine"}, SendFeatureFlags: true,
		FlagOptions: FlagOptions{AppContexts: []string{"api"}}})
	ev := c.Captured()[0]
	if ev.Properties["$feature/a"] != "mine" || ev.Properties["$feature/off"] != false || ev.Properties["$feature/web"] != nil {
		t.Fatalf("%v", ev.Properties)
	}
	if got := ev.Properties["$active_feature_flags"]; !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("active %v", got)
	}

	snap := c.EvaluateFlags(ctx, "u", FlagOptions{})
	if !snap.IsEnabled("a") || snap.Keys()[0] != "a" {
		t.Fatal(snap.Keys())
	}
	c.Capture(CaptureInput{DistinctID: "u", Event: "e1", Flags: snap.OnlyAccessed()})
	got := c.Captured()[1].Properties
	if got["$feature/a"] != true || got["$feature/b"] != nil {
		t.Fatalf("only accessed: %v", got)
	}
	for _, e := range c.Captured() {
		if e.Event == "$feature_flag_called" {
			t.Fatal("snapshot reads must not emit exposures")
		}
	}
}

func TestGetFeatureFlagDefaultValue(t *testing.T) {
	c := NewTestClientWithFlags(snapFlags())
	if v, ok := c.GetFeatureFlag(context.Background(), "missing", "u", FlagOptions{OnlyEvaluateLocally: true, DefaultValue: "control"}); ok || v != "control" {
		t.Fatalf("%v %v", v, ok)
	}
}
