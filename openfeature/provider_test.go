package lucidaprovider

import (
	"context"
	"testing"

	"github.com/open-feature/go-sdk/openfeature"

	lucida "github.com/lucidaanalytics/lucida-go"
)

func fixture() *lucida.Client {
	return lucida.NewTestClientWithFlags([]lucida.Flag{
		{Key: "on", Type: "boolean", Active: true, Salt: "s", Conditions: []lucida.Condition{{RolloutPct: 100}}},
		{Key: "cfg", Type: "json", Active: true, Salt: "s", Payload: map[string]any{"n": 3.0}, Conditions: []lucida.Condition{{RolloutPct: 100}}},
	})
}

func TestProviderThroughOpenFeature(t *testing.T) {
	if err := openfeature.SetProviderAndWait(New(fixture())); err != nil {
		t.Fatal(err)
	}
	c := openfeature.NewClient("t")
	ec := openfeature.NewEvaluationContext("u1", map[string]any{"plan": "pro"})
	ctx := context.Background()
	if v, err := c.BooleanValue(ctx, "on", false, ec); err != nil || !v {
		t.Fatalf("bool %v %v", v, err)
	}
	if v, err := c.BooleanValue(ctx, "missing", true, ec); err == nil || !v {
		t.Fatalf("missing must error and return default, got %v %v", v, err)
	}
	if v, _ := c.ObjectValue(ctx, "cfg", nil, ec); v == nil {
		t.Fatal("object payload not resolved")
	}
	if _, err := c.BooleanValue(ctx, "on", false, openfeature.EvaluationContext{}); err == nil {
		t.Fatal("missing targeting key must error")
	}
}
