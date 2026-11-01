package lucida

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestStickyStoreKeepsAssignment(t *testing.T) {
	f := Flag{Key: "f", Type: TypeMultivariate, Active: true, Salt: "s", Sticky: true,
		Aggregation: Aggregation{Kind: "person"}, Conditions: []Condition{{RolloutPct: 100}},
		Variants: []Variant{{Key: "control", RolloutPct: 100}, {Key: "test", RolloutPct: 0}}}
	store := NewMemoryStickyStore()
	c := EvalContext{DistinctID: "u1"}
	if r := EvaluateAllSticky([]Flag{f}, c, store)["f"]; r.Value != "control" || r.Reason != ReasonConditionMatch {
		t.Fatalf("first: %+v", r)
	}
	if got := store.Get("u1"); !reflect.DeepEqual(got, map[string]any{"f": "control"}) {
		t.Fatalf("stored %v", got)
	}
	f.Variants = []Variant{{Key: "control", RolloutPct: 0}, {Key: "test", RolloutPct: 100}}
	if r := EvaluateAllSticky([]Flag{f}, c, store)["f"]; r.Value != "control" || r.Reason != ReasonStickyBucket {
		t.Fatalf("after reweight: %+v", r)
	}
	f.Sticky = false
	if r := EvaluateAllSticky([]Flag{f}, c, store)["f"]; r.Value != "test" {
		t.Fatalf("non-sticky: %+v", r)
	}
	off := Flag{Key: "g", Type: TypeBoolean, Active: true, Salt: "s", Sticky: true,
		Aggregation: Aggregation{Kind: "person"}, Conditions: []Condition{{RolloutPct: 0}}}
	EvaluateAllSticky([]Flag{off}, c, store)
	if _, ok := store.Get("u1")["g"]; ok {
		t.Fatal("false must not be stored")
	}
}

func TestMigrationStages(t *testing.T) {
	if MigrationStageOf("live") != MigrationLive || MigrationStageOf(true) != MigrationOff || MigrationStageOf("x") != MigrationOff {
		t.Fatal("MigrationStageOf")
	}
	var calls []string
	var rep MigrationReport
	ops := MigrationOps[int, string]{
		ReadOld:  func(context.Context) (int, error) { return 1, nil },
		ReadNew:  func(context.Context) (int, error) { return 2, nil },
		WriteOld: func(context.Context) (string, error) { calls = append(calls, "old"); return "o", nil },
		WriteNew: func(context.Context) (string, error) { calls = append(calls, "new"); return "", errors.New("boom") },
		OnReport: func(r MigrationReport) { rep = r },
	}
	ctx := context.Background()
	reads := map[MigrationStage]int{MigrationOff: 1, MigrationDualwrite: 1, MigrationShadow: 1, MigrationLive: 2, MigrationRampdown: 2, MigrationComplete: 2}
	for s, want := range reads {
		if v, err := MigrateRead(ctx, s, ops); v != want || err != nil {
			t.Fatalf("read %s = %v %v", s, v, err)
		}
		both := s == MigrationShadow || s == MigrationLive
		if (rep.Consistent != nil) != both || (both && *rep.Consistent) || len(rep.LatencyMS) != map[bool]int{true: 2, false: 1}[both] {
			t.Fatalf("read report %s: %+v", s, rep)
		}
	}
	ops.Compare = func(a, b int) bool { return true }
	if MigrateRead(ctx, MigrationShadow, ops); !*rep.Consistent {
		t.Fatal("Compare not used")
	}
	writes := map[MigrationStage][]string{
		MigrationOff: {"old"}, MigrationDualwrite: {"old", "new"}, MigrationShadow: {"old", "new"},
		MigrationLive: {"new"}, MigrationRampdown: {"new"}, MigrationComplete: {"new"},
	}
	for s, want := range writes {
		calls = nil
		v, err := MigrateWrite(ctx, s, ops)
		if !reflect.DeepEqual(calls, want) {
			t.Fatalf("write %s calls %v, want %v", s, calls, want)
		}
		if (want[0] == "old") != (err == nil && v == "o") {
			t.Fatalf("write %s = %q %v", s, v, err)
		}
		if rep.Op != "write" || (rep.Errors["new"] == "boom") != (want[len(want)-1] == "new") {
			t.Fatalf("write report %s: %+v", s, rep)
		}
	}
}

func TestAIConfigFrom(t *testing.T) {
	fb := AIConfig{Model: "fallback-model", Messages: []AIMessage{{"system", "hi {{name}}"}}}
	got := AIConfigFrom(nil, fb, map[string]string{"name": "Ann"})
	if got.Enabled || got.Model != "fallback-model" || got.Messages[0].Content != "hi Ann" {
		t.Fatalf("fallback: %+v", got)
	}
	r := &FlagResult{Value: "v2", Variant: "v2", Payload: map[string]any{
		"model": "m", "provider": "p", "version": 3.0, "parameters": map[string]any{"temperature": 0.2},
		"messages": []any{map[string]any{"role": "user", "content": "{{ a }} and {{missing}}"}},
	}}
	got = AIConfigFrom(r, fb, map[string]string{"a": "x"})
	if !got.Enabled || got.Variation != "v2" || got.Model != "m" || got.Provider != "p" || *got.Version != 3 ||
		got.Parameters["temperature"] != 0.2 || !reflect.DeepEqual(got.Messages, []AIMessage{{"user", "x and {{missing}}"}}) {
		t.Fatalf("served: %+v", got)
	}
	if AIConfigFrom(&FlagResult{Value: true, Payload: r.Payload}, fb, nil).Enabled {
		t.Fatal("non-variant result must fall back")
	}
}
