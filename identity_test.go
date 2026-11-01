package lucida

import "testing"

func TestRegisterSetPersonAndDedupe(t *testing.T) {
	c := NewTestClientWithFlags(nil)
	c.Register(map[string]any{"app": "a", "env": "prod"})
	c.Unregister("env")
	c.Capture(CaptureInput{DistinctID: "u", Event: "e", Properties: map[string]any{"app": "mine"}})
	c.Capture(CaptureInput{DistinctID: "u", Event: "e2"})
	c.SetPersonProperties("u", map[string]any{"plan": "pro"})
	c.SetPersonPropertiesOnce("u", map[string]any{"first": "x"})
	for i := 0; i < 3; i++ {
		c.Identify("u", map[string]any{"a": 1}, nil)
		c.GroupIdentify("org", "acme", map[string]any{"n": 1}, "")
	}
	c.Identify("u", map[string]any{"a": 2}, nil)
	ev := c.Captured()
	if ev[0].Properties["app"] != "mine" || ev[1].Properties["app"] != "a" {
		t.Fatalf("per-call must win over super: %v %v", ev[0].Properties, ev[1].Properties)
	}
	if _, has := ev[1].Properties["env"]; has {
		t.Fatal("unregistered key still sent")
	}
	if ev[2].Event != "$set" || ev[2].Set["plan"] != "pro" || ev[3].SetOnce["first"] != "x" {
		t.Fatalf("person property events: %+v %+v", ev[2], ev[3])
	}
	var names []string
	for _, e := range ev[4:] {
		names = append(names, e.Event)
	}
	if len(names) != 3 || names[0] != "$identify" || names[1] != "$groupidentify" || names[2] != "$identify" {
		t.Fatalf("dedupe: %v", names)
	}
}
