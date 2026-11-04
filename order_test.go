package lucida

import (
	"testing"
	"time"
)

func TestCaptureKeepsCallOrderBehindRemoteFlags(t *testing.T) {
	f := newFake(t)
	f.respond = func(path string, _ int) (int, map[string]string) {
		if path == "/v1/flags/definitions" {
			return 503, nil // no local definitions: the flags are evaluated remotely
		}
		return 0, nil
	}
	f.remoteOut = `{"request_id":"r1","flags":{"cohorted":{"enabled":true,"value":true}}}`
	c := New("lc_pk", Config{Host: f.URL, SecretKey: "lc_sk", FlagPollInterval: time.Hour})
	<-c.Ready()
	c.Capture(CaptureInput{DistinctID: "u1", Event: "first", SendFeatureFlags: true, FlagOptions: FlagOptions{AnonDistinctID: "a"}})
	c.Capture(CaptureInput{DistinctID: "u1", Event: "second"})
	if err := c.Flush(ctx5()); err != nil {
		t.Fatal(err)
	}
	if len(f.to("/v1/flags")) == 0 {
		t.Fatal("flags were not evaluated remotely")
	}
	var got []string
	for _, b := range f.bodies("/v1/batch") {
		for _, e := range b["batch"].([]any) {
			got = append(got, e.(map[string]any)["event"].(string))
		}
	}
	if len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Fatalf("order %v", got)
	}
	_ = c.Shutdown(ctx5())
}
