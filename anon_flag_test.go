package lucida

import (
	"testing"
	"time"
)

func TestRemoteFlagRequestCarriesAnonDistinctID(t *testing.T) {
	f := newFake(t)
	f.flagDefs = flagDefs
	f.remoteOut = `{"request_id":"r1","flags":{"cohorted":{"enabled":true,"value":true}}}`
	c := New("lc_pk", Config{Host: f.URL, SecretKey: "lc_sk", FlagPollInterval: time.Hour})
	<-c.Ready()
	c.GetFeatureFlag(ctx5(), "cohorted", "u1", FlagOptions{NoExposure: true, AnonDistinctID: "anon-9"})
	c.GetFeatureFlag(ctx5(), "cohorted", "u2", FlagOptions{NoExposure: true})
	bodies := f.bodies("/v1/flags")
	if len(bodies) != 2 || bodies[0]["anon_distinct_id"] != "anon-9" {
		t.Fatalf("%v", bodies)
	}
	if _, has := bodies[1]["anon_distinct_id"]; has {
		t.Fatalf("absent when unset: %v", bodies[1])
	}
	_ = c.Shutdown(ctx5())
}
