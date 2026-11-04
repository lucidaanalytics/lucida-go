package lucida

import (
	"context"
	"net/http"
	"strings"
)

// Tracing headers the browser SDK adds to requests to your own backend (option tracingHeaders).
const (
	DistinctIDHeader = "X-Lucida-Distinct-Id"
	SessionIDHeader  = "X-Lucida-Session-Id"
)

// RequestContext is the ambient identity for events captured with CaptureContext.
type RequestContext struct {
	DistinctID string
	SessionID  string         // sent as $session_id
	Properties map[string]any // merged under the event's own properties
	// Fresh starts from an empty context instead of inheriting the one already in ctx.
	Fresh bool
}

type ctxKey struct{}

// WithRequestContext returns a child context carrying rc merged over the parent's (unless rc.Fresh).
// Non-empty fields of rc win; properties are merged key by key. The parent is never modified.
func WithRequestContext(ctx context.Context, rc RequestContext) context.Context {
	var base RequestContext
	if !rc.Fresh {
		base, _ = RequestContextFrom(ctx)
	}
	out := RequestContext{DistinctID: base.DistinctID, SessionID: base.SessionID,
		Properties: make(map[string]any, len(base.Properties)+len(rc.Properties))}
	if rc.DistinctID != "" {
		out.DistinctID = rc.DistinctID
	}
	if rc.SessionID != "" {
		out.SessionID = rc.SessionID
	}
	for _, m := range []map[string]any{base.Properties, rc.Properties} {
		for k, v := range m {
			out.Properties[k] = v
		}
	}
	return context.WithValue(ctx, ctxKey{}, out)
}

// RequestContextFrom returns the context set by WithRequestContext.
func RequestContextFrom(ctx context.Context) (RequestContext, bool) {
	rc, ok := ctx.Value(ctxKey{}).(RequestContext)
	return rc, ok
}

// CleanHeader strips ASCII control characters, trims, and caps at 1000 characters.
func CleanHeader(v string) string {
	v = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, v)
	v = strings.TrimSpace(v)
	if len(v) > 1000 {
		v = v[:1000]
	}
	return v
}

// ReadTracingHeaders returns the distinct id and session id the browser SDK sent ("" when absent).
// These headers are client-controlled: use them to join events to a browser session, never for authentication.
func ReadTracingHeaders(h http.Header) (distinctID, sessionID string) {
	return CleanHeader(h.Get(DistinctIDHeader)), CleanHeader(h.Get(SessionIDHeader))
}

// CaptureContext is Capture that fills a missing DistinctID, the session id and extra properties from the
// RequestContext in ctx. Explicit fields of in always win.
func (c *Client) CaptureContext(ctx context.Context, in CaptureInput) {
	rc, _ := RequestContextFrom(ctx)
	if in.DistinctID == "" {
		in.DistinctID = rc.DistinctID
	}
	if in.DistinctID == "" {
		if !in.Personless {
			c.fail(errNoDistinctID, nil)
			return
		}
		in.DistinctID = uuidV7()
	}
	props := make(map[string]any, len(rc.Properties)+len(in.Properties)+4)
	c.identity.copySuper(props)
	for k, v := range rc.Properties {
		props[k] = v
	}
	if rc.SessionID != "" {
		props["$session_id"] = rc.SessionID
	}
	for k, v := range in.Properties {
		props[k] = v
	}
	in.Properties = props
	if in.Personless {
		props["$process_person_profile"] = false
		in.Set, in.SetOnce = nil, nil
	}
	if geo := c.cfg.DisableGeoip; (in.DisableGeoip == nil && geo) || (in.DisableGeoip != nil && *in.DisableGeoip) {
		props["$geoip_disable"] = true
	}
	in, remote := c.withFlags(in)
	prev, mine := c.order.enter(remote != nil)
	if mine == nil {
		c.emit(in)
		return
	}
	go func() {
		if remote != nil {
			in = remote()
		}
		if prev != nil {
			<-prev
		}
		c.emit(in)
		c.order.leave(mine)
	}()
}

// emit runs beforeSend and hands the event to the queue (or the test sink).
func (c *Client) emit(in CaptureInput) {
	e, keep := c.beforeSend(newWire(in))
	if !keep {
		return
	}
	if c.sink != nil {
		c.sink.add(e)
	} else if c.queue != nil {
		c.queue.add(e)
	}
}

// beforeSend runs the user hook: nil drops, a panic keeps the original event.
func (c *Client) beforeSend(e wireEvent) (out wireEvent, keep bool) {
	if c.cfg.BeforeSend == nil {
		return e, true
	}
	defer func() {
		if r := recover(); r != nil {
			c.fail(errBeforeSendPanic, []wireEvent{e})
			out, keep = e, true
		}
	}()
	edited := e // the hook gets its own copy of the properties, so a panic cannot leave the original half-edited
	edited.Properties = make(map[string]any, len(e.Properties))
	for k, v := range e.Properties {
		edited.Properties[k] = v
	}
	if res := c.cfg.BeforeSend(edited); res != nil {
		return *res, true
	}
	return e, false
}

func (c *Client) fail(err error, events []wireEvent) {
	if c.cfg.OnError != nil {
		defer func() { _ = recover() }() // a panicking callback must not break delivery
		c.cfg.OnError(err, events)
	}
}

func (c *Client) batchDropped(batch []wireEvent, status int) {
	c.fail(&DeliveryError{Status: status, Dropped: len(batch)}, batch)
}
