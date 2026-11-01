package lucida

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
)

// CapturedEvent is an event recorded by a test client (the wire form that would have been sent).
type CapturedEvent = wireEvent

// testSink collects events in memory for assertions.
type testSink struct {
	mu     sync.Mutex
	events []wireEvent
}

func (s *testSink) add(e wireEvent) {
	s.mu.Lock()
	s.events = append(s.events, e)
	s.mu.Unlock()
}

type noNetwork struct{}

func (noNetwork) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("lucida: network disabled in test mode")
}

// NewTestClient returns an offline client for tests: no network and no goroutines, flags evaluate locally from the
// fixture file written by `lucida flags fixture` (`{"flags": [...]}`), and events are kept for Captured().
// Flags missing from the fixture or needing the server (cohorts) read as not found.
func NewTestClient(fixtureFile string) (*Client, error) {
	raw, err := os.ReadFile(fixtureFile)
	if err != nil {
		return nil, err
	}
	var body struct {
		Flags *[]Flag `json:"flags"`
	}
	if err := json.Unmarshal(raw, &body); err != nil || body.Flags == nil {
		return nil, fmt.Errorf(`lucida: fixture %s must be {"flags": [...]}`, fixtureFile)
	}
	return NewTestClientWithFlags(*body.Flags), nil
}

// NewTestClientWithFlags is NewTestClient with definitions built in code.
func NewTestClientWithFlags(flags []Flag) *Client {
	// Disabled makes New skip the queue and goroutines; reads and captures are re-enabled below.
	c := New("lc_pk_test", Config{Disabled: true, HTTPClient: &http.Client{Transport: noNetwork{}}})
	c.cfg.Disabled = false
	c.defs, c.haveDefs = flags, true
	c.sink = &testSink{}
	return c
}

// Captured returns a copy of every event recorded so far, including $feature_flag_called and $exposure.
// It returns nil on a normal client.
func (c *Client) Captured() []CapturedEvent {
	if c.sink == nil {
		return nil
	}
	c.sink.mu.Lock()
	defer c.sink.mu.Unlock()
	return append([]CapturedEvent{}, c.sink.events...)
}
