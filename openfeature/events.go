package lucidaprovider

import (
	"context"
	"sync"

	"github.com/open-feature/go-sdk/openfeature"

	lucida "github.com/lucidaanalytics/lucida-go"
)

// events turns the Lucida client's reload hooks into OpenFeature provider events.
type events struct {
	ch       chan openfeature.Event
	mu       sync.Mutex
	loaded   bool  // a definitions load succeeded
	isStale  bool  // the last signal was a failure after a success
	firstErr error // the first load failed
}

func newEvents() events { return events{ch: make(chan openfeature.Event, 16)} }

func (e *events) emit(t openfeature.EventType, msg string) {
	select { // never block the loader; an overflowing consumer misses events
	case e.ch <- openfeature.Event{ProviderName: "lucida", EventType: t, ProviderEventDetails: openfeature.ProviderEventDetails{Message: msg}}:
	default:
	}
}

func (e *events) changed() {
	e.mu.Lock()
	first, recovered := !e.loaded, e.isStale
	e.loaded, e.isStale, e.firstErr = true, false, nil
	e.mu.Unlock()
	switch {
	case recovered:
		e.emit(openfeature.ProviderReady, "flag definitions reloaded")
	case !first: // the first load is announced by Init returning (the SDK emits READY)
		e.emit(openfeature.ProviderConfigChange, "flag definitions changed")
	}
}

func (e *events) stale(err error) {
	e.mu.Lock()
	if !e.loaded {
		e.firstErr = err
		e.mu.Unlock()
		e.emit(openfeature.ProviderError, err.Error())
		return
	}
	already := e.isStale
	e.isStale = true
	e.mu.Unlock()
	if !already {
		e.emit(openfeature.ProviderStale, err.Error())
	}
}

// EventChannel is the OpenFeature provider event stream.
func (p *Provider) EventChannel() <-chan openfeature.Event { return p.ev.ch }

// Init waits for the first definitions load when the client has one (Ready); a failed first load is an error.
func (p *Provider) Init(openfeature.EvaluationContext) error {
	if r, ok := p.flags.(interface{ Ready() <-chan struct{} }); ok {
		<-r.Ready()
	}
	p.ev.mu.Lock()
	defer p.ev.mu.Unlock()
	return p.ev.firstErr
}

func (p *Provider) Shutdown() { p.flags.OnFlagsChange(nil, nil) }

// Track maps the OpenFeature tracking API to a Lucida event: the tracking event name, the targeting key as
// distinct id, the details' attributes as properties and `$value` = details.Value() when non-zero.
func (p *Provider) Track(_ context.Context, name string, ec openfeature.EvaluationContext, details openfeature.TrackingEventDetails) {
	id := ec.TargetingKey()
	if id == "" {
		return
	}
	props := details.Attributes()
	if v := details.Value(); v != 0 {
		props["$value"] = v
	}
	fc := openfeature.FlattenedContext{openfeature.TargetingKey: id}
	for k, v := range ec.Attributes() {
		fc[k] = v
	}
	_, o := splitContext(fc)
	p.flags.Capture(lucida.CaptureInput{DistinctID: id, Event: name, Properties: props, Groups: o.Groups})
}

// splitContext maps a flattened OpenFeature context to the distinct id and Lucida flag options.
func splitContext(fc openfeature.FlattenedContext) (string, lucida.FlagOptions) {
	id, _ := fc[openfeature.TargetingKey].(string)
	o := lucida.FlagOptions{PersonProperties: map[string]any{}}
	for k, v := range fc {
		switch k {
		case openfeature.TargetingKey:
		case "groups":
			o.Groups = stringMap(v)
		case "groupProperties":
			o.GroupProperties = nestedMap(v)
		default:
			o.PersonProperties[k] = v
		}
	}
	return id, o
}

func stringMap(v any) map[string]string {
	switch m := v.(type) {
	case map[string]string:
		return m
	case map[string]any:
		out := make(map[string]string, len(m))
		for k, x := range m {
			if s, ok := x.(string); ok {
				out[k] = s
			}
		}
		return out
	}
	return nil
}

func nestedMap(v any) map[string]map[string]any {
	switch m := v.(type) {
	case map[string]map[string]any:
		return m
	case map[string]any:
		out := make(map[string]map[string]any, len(m))
		for k, x := range m {
			if inner, ok := x.(map[string]any); ok {
				out[k] = inner
			}
		}
		return out
	}
	return nil
}
