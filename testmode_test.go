package lucida

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

const fixtureJSON = `{"flags":[
 {"key":"on","type":"boolean","active":true,"salt":"s","aggregation":{"kind":"person"},"conditions":[{"rollout_pct":100}]},
 {"key":"off","type":"boolean","active":false,"salt":"s","aggregation":{"kind":"person"},"conditions":[{"rollout_pct":100}]}
]}`

func TestTestClientOfflineAndSink(t *testing.T) {
	file := filepath.Join(t.TempDir(), "flags.json")
	if err := os.WriteFile(file, []byte(fixtureJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	c, err := NewTestClient(file)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if v, ok := c.GetFeatureFlag(ctx, "on", "u1", FlagOptions{}); !ok || v != true {
		t.Fatalf("on = %v %v", v, ok)
	}
	if v, ok := c.GetFeatureFlag(ctx, "off", "u1", FlagOptions{}); !ok || v != false {
		t.Fatalf("off = %v %v", v, ok)
	}
	if _, ok := c.GetFeatureFlag(ctx, "missing", "u1", FlagOptions{}); ok {
		t.Fatal("a flag outside the fixture must not be found (and must not call the network)")
	}
	c.GetFeatureFlag(ctx, "on", "u1", FlagOptions{}) // exposure dedupes per (user, flag, value)
	c.Capture(CaptureInput{DistinctID: "u1", Event: "signup", Properties: map[string]any{"plan": "pro"}})
	if n, err := c.ImportEvents(ctx, []ImportEvent{{DistinctID: "u2", Event: "old"}}); err != nil || n != 1 {
		t.Fatalf("import = %d %v", n, err)
	}
	if err := c.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range c.Captured() {
		names = append(names, e.Event)
	}
	want := []string{"$feature_flag_called", "$exposure", "$feature_flag_called", "$exposure", "signup", "old"}
	if len(names) != len(want) {
		t.Fatalf("events = %v", names)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("events = %v", names)
		}
	}
	if got := c.Captured()[4].Properties["plan"]; got != "pro" {
		t.Fatalf("plan = %v", got)
	}
	if all := c.GetAllFlags(ctx, "u1", FlagOptions{}); all["on"] != true || all["off"] != false || len(all) != 2 {
		t.Fatalf("all = %v", all)
	}
	if err := c.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestTestClientRejectsBadFixture(t *testing.T) {
	file := filepath.Join(t.TempDir(), "bad.json")
	_ = os.WriteFile(file, []byte(`{"nope":1}`), 0o644)
	if _, err := NewTestClient(file); err == nil {
		t.Fatal("must reject")
	}
	if _, err := NewTestClient(filepath.Join(t.TempDir(), "absent.json")); err == nil {
		t.Fatal("must reject a missing file")
	}
	if (&Client{}).Captured() != nil {
		t.Fatal("a normal client captures nothing in memory")
	}
}
