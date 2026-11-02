package lucida

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// FlagOptions are the per-call inputs of the flag reads.
type FlagOptions struct {
	PersonProperties map[string]any
	Groups           map[string]string
	GroupProperties  map[string]map[string]any
	// OnlyEvaluateLocally never calls the server: flags undecidable locally (cohorts, unknown) are not found.
	OnlyEvaluateLocally bool
	// NoExposure suppresses $feature_flag_called / $exposure (GetFeatureFlag only).
	NoExposure bool
	// AppContexts this call declares (default Config.AppContexts); flags limited to others are not served.
	AppContexts []string
	// AnonDistinctID is the pre-identify id of this person; the gateway links it to the distinct id so
	// flags decided for the anonymous visitor keep their bucket after identify (LC-FLG-34).
	AnonDistinctID string
	// DefaultValue is what GetFeatureFlag returns (with ok false) when nothing decides the flag:
	// cold start before definitions load, a network error, or an unknown key.
	DefaultValue any
}

type resolved struct {
	value     any
	payload   any
	locally   bool
	requestID string
	reason    string
	condIdx   int
}

func (o FlagOptions) evalContext(id string) EvalContext {
	return EvalContext{DistinctID: id, Groups: o.Groups, PersonProperties: o.PersonProperties, GroupProperties: o.GroupProperties, AppContexts: o.AppContexts}
}

func (c *Client) opts(o FlagOptions) FlagOptions {
	if o.AppContexts == nil {
		o.AppContexts = c.cfg.AppContexts
	}
	return o
}

// poll loads definitions now and then every FlagPollInterval until Shutdown.
func (c *Client) poll() {
	t := time.NewTicker(c.cfg.FlagPollInterval)
	defer t.Stop()
	for {
		if !c.streaming.Load() {
			_ = c.ReloadFeatureFlags(context.Background())
		}
		c.readyOne.Do(func() { close(c.ready) })
		select {
		case <-t.C:
		case <-c.stopPoll:
			return
		}
	}
}

// Ready is closed after the first definitions fetch attempt (immediately without a SecretKey).
func (c *Client) Ready() <-chan struct{} { return c.ready }

// ReloadFeatureFlags fetches flag definitions now (ETag aware). The previous definitions are kept on error.
func (c *Client) ReloadFeatureFlags(ctx context.Context) error {
	changed, err := c.fetchDefinitions(ctx)
	c.flagMu.RLock()
	onChange, onStale := c.onChange, c.onStale
	c.flagMu.RUnlock()
	if err != nil && onStale != nil {
		onStale(err)
	} else if changed && onChange != nil {
		onChange()
	}
	return err
}

// OnFlagsChange registers callbacks: changed runs after a reload installed new definitions, stale after a
// reload (or the flag stream) failed. They run on the loading goroutine; keep them quick. Nil clears one.
func (c *Client) OnFlagsChange(changed func(), stale func(error)) {
	c.flagMu.Lock()
	c.onChange, c.onStale = changed, stale
	c.flagMu.Unlock()
}

func (c *Client) fireStale(err error) {
	c.flagMu.RLock()
	f := c.onStale
	c.flagMu.RUnlock()
	if f != nil {
		f(err)
	}
}

// fetchDefinitions is one ETag-aware fetch; changed is true when new definitions were installed.
func (c *Client) fetchDefinitions(ctx context.Context) (changed bool, err error) {
	if c.secret == "" || c.cfg.Disabled {
		return false, nil
	}
	if defs, ok := adoptCachedFlags(ctx, c.cfg.FlagDefinitionCache); ok {
		c.flagMu.Lock()
		c.defs, c.haveDefs = defs, true
		c.flagMu.Unlock()
		return true, nil
	}
	c.flagMu.RLock()
	etag := c.etag
	c.flagMu.RUnlock()
	ctx, cancel := context.WithTimeout(ctx, c.cfg.FlagTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.host+"/v1/flags/definitions", nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Authorization", "Bearer "+c.secret)
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		c.logf("flag definitions poll failed: %v", err)
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotModified {
		return false, nil
	}
	if resp.StatusCode != http.StatusOK {
		c.logf("flag definitions poll: status %d", resp.StatusCode)
		return false, fmt.Errorf("lucida: flag definitions: status %d", resp.StatusCode)
	}
	var body struct {
		Flags *[]Flag `json:"flags"`
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err := json.Unmarshal(raw, &body); err != nil || body.Flags == nil {
		c.logf("flag definitions poll: bad body")
		return false, fmt.Errorf("lucida: flag definitions: bad body")
	}
	c.flagMu.Lock()
	c.defs, c.haveDefs, c.etag = *body.Flags, true, resp.Header.Get("ETag")
	c.flagMu.Unlock()
	publishCachedFlags(ctx, c.cfg.FlagDefinitionCache, *body.Flags)
	return true, nil
}

func (c *Client) local(id string, o FlagOptions) map[string]FlagResult {
	o = c.opts(o)
	c.flagMu.RLock()
	defs, ok := c.defs, c.haveDefs
	c.flagMu.RUnlock()
	if !ok {
		return nil
	}
	var all map[string]FlagResult
	if c.cfg.StickyStore != nil {
		all = EvaluateAllSticky(defs, o.evalContext(id), c.cfg.StickyStore)
	} else {
		all = EvaluateAll(defs, o.evalContext(id))
	}
	for k, r := range all {
		if r.Reason == ReasonAppContextExcluded {
			delete(all, k)
		}
	}
	return all
}

// GetRemoteConfigPayload returns the payload of a remote-config flag (GET /v1/flags/{key}/remote-config),
// decrypted when it is stored encrypted (the gateway opens those for secret keys only). It needs SecretKey;
// ok is false when the flag is unknown, not a remote-config flag, or the call fails.
func (c *Client) GetRemoteConfigPayload(ctx context.Context, key string) (payload any, ok bool, err error) {
	if c.secret == "" {
		return nil, false, errors.New("lucida: GetRemoteConfigPayload requires SecretKey")
	}
	if c.cfg.Disabled {
		return nil, false, nil
	}
	status, _, raw := c.doTimeout(ctx, c.cfg.FlagTimeout, http.MethodGet, c.host+"/v1/flags/"+url.PathEscape(key)+"/remote-config", nil,
		map[string]string{"Authorization": "Bearer " + c.secret})
	var resp struct {
		Payload any `json:"payload"`
	}
	if status != http.StatusOK || json.Unmarshal(raw, &resp) != nil {
		if status != http.StatusNotFound {
			c.logf("remote config failed: status %d", status)
		}
		return nil, false, nil
	}
	return resp.Payload, true, nil
}

func (c *Client) remote(ctx context.Context, id string, o FlagOptions, keys []string) map[string]resolved {
	o = c.opts(o)
	req := map[string]any{
		"api_key": c.apiKey, "distinct_id": id,
		"groups": orEmpty(o.Groups), "person_properties": orEmpty(o.PersonProperties),
		"group_properties": orEmpty(o.GroupProperties),
	}
	if len(keys) > 0 {
		req["flag_keys"] = keys
	}
	if len(o.AppContexts) > 0 {
		req["app_contexts"] = o.AppContexts
	}
	if o.AnonDistinctID != "" {
		req["anon_distinct_id"] = o.AnonDistinctID
	}
	status, _, raw := c.doTimeout(ctx, c.cfg.FlagTimeout, http.MethodPost, c.host+"/v1/flags", req, nil)
	out := map[string]resolved{}
	var resp struct {
		Flags map[string]struct {
			Enabled bool   `json:"enabled"`
			Variant string `json:"variant"`
			Value   any    `json:"value"`
			Payload any    `json:"payload"`
			Reason  string `json:"reason"`
			// ConditionIndex is -1 when no condition decided the result.
			ConditionIndex *int `json:"condition_index"`
		} `json:"flags"`
		RequestID    string `json:"request_id"`
		QuotaLimited bool   `json:"quota_limited"`
	}
	if status != http.StatusOK || json.Unmarshal(raw, &resp) != nil {
		c.logf("remote flags failed: status %d", status)
		return out
	}
	if resp.QuotaLimited {
		c.logf("flags quota limited")
	}
	for k, f := range resp.Flags {
		v := f.Value
		if v == nil {
			v = f.Enabled
			if f.Variant != "" {
				v = f.Variant
			}
		}
		ci := -1
		if f.ConditionIndex != nil {
			ci = *f.ConditionIndex
		}
		out[k] = resolved{v, f.Payload, false, resp.RequestID, f.Reason, ci}
	}
	return out
}

func orEmpty[M ~map[K]V, K comparable, V any](m M) M {
	if m == nil {
		return M{}
	}
	return m
}

func (c *Client) resolve(ctx context.Context, key, id string, o FlagOptions) (resolved, bool) {
	if r, ok := c.local(id, o)[key]; ok && r.Reason != ReasonRequiresServer {
		return resolved{r.Value, r.Payload, true, "", r.Reason, r.ConditionIndex}, true
	}
	if !o.OnlyEvaluateLocally {
		if r, ok := c.remote(ctx, id, o, []string{key})[key]; ok {
			return r, true
		}
	}
	// Nothing decided the flag: the init-time Fallbacks value, if any (nothing was assigned, so no exposure).
	if v, ok := c.cfg.Fallbacks[key]; ok {
		return resolved{value: v, locally: true, reason: ReasonFallback, condIdx: -1}, true
	}
	return resolved{}, false
}

// GetFeatureFlag returns the flag value (false, true or a variant key); ok is false when the flag is
// unknown or undecidable locally with OnlyEvaluateLocally. It emits $feature_flag_called and $exposure
// once per (distinctID, flag, value).
func (c *Client) GetFeatureFlag(ctx context.Context, key, distinctID string, o FlagOptions) (value any, ok bool) {
	if c.cfg.Disabled {
		return nil, false
	}
	r, ok := c.resolve(ctx, key, distinctID, o)
	if !ok {
		return o.DefaultValue, false
	}
	if !o.NoExposure {
		c.exposure(distinctID, key, r)
	}
	return r.value, true
}

// exposure emits $feature_flag_called and $exposure once per (distinctID, flag, value); fallbacks emit nothing.
func (c *Client) exposure(distinctID, key string, r resolved) {
	if r.reason == ReasonFallback || !c.seen.add(fmt.Sprintf("%s\x00%s\x00%v", distinctID, key, r.value)) {
		return
	}
	props := map[string]any{"$feature_flag": key, "$feature_flag_response": r.value, "locally_evaluated": r.locally}
	if r.requestID != "" {
		props["$feature_flag_request_id"] = r.requestID
	}
	for _, ev := range []string{"$feature_flag_called", "$exposure"} {
		p := make(map[string]any, len(props))
		for k, v := range props {
			p[k] = v
		}
		c.Capture(CaptureInput{DistinctID: distinctID, Event: ev, Properties: p})
	}
}

// FlagDetails is a flag read with the evidence behind it.
type FlagDetails struct {
	Key, Variant string
	Value        any
	Payload      any
	// Reason is the raw Lucida reason (ReasonConditionMatch, ... or ReasonFallback).
	Reason string
	// ConditionIndex is the deciding condition, -1 when none.
	ConditionIndex   int
	RequestID        string
	EvaluatedLocally bool
}

// GetFeatureFlagDetails is GetFeatureFlag with payload, reason and provenance (same exposure events
// unless o.NoExposure; never for fallbacks). ok is false when nothing (not even a Config.Fallbacks entry) decides the flag.
func (c *Client) GetFeatureFlagDetails(ctx context.Context, key, distinctID string, o FlagOptions) (FlagDetails, bool) {
	if c.cfg.Disabled {
		return FlagDetails{}, false
	}
	r, ok := c.resolve(ctx, key, distinctID, o)
	if !ok {
		return FlagDetails{}, false
	}
	if !o.NoExposure {
		c.exposure(distinctID, key, r)
	}
	v, _ := r.value.(string)
	return FlagDetails{Key: key, Value: r.value, Payload: r.payload, Variant: v, Reason: r.reason,
		ConditionIndex: r.condIdx, RequestID: r.requestID, EvaluatedLocally: r.locally}, true
}

// IsFeatureEnabled is GetFeatureFlag reduced to "on" (true or any variant).
func (c *Client) IsFeatureEnabled(ctx context.Context, key, distinctID string, o FlagOptions) (enabled, ok bool) {
	v, ok := c.GetFeatureFlag(ctx, key, distinctID, o)
	return ok && v != false, ok
}

// GetFeatureFlagPayload returns the payload of the flag's value, or nil. It never emits events.
func (c *Client) GetFeatureFlagPayload(ctx context.Context, key, distinctID string, o FlagOptions) any {
	if c.cfg.Disabled {
		return nil
	}
	r, _ := c.resolve(ctx, key, distinctID, o)
	return r.payload
}

// GetAllFlags evaluates every flag, locally where possible and remotely for the rest. It never emits events.
func (c *Client) GetAllFlags(ctx context.Context, distinctID string, o FlagOptions) map[string]any {
	if c.cfg.Disabled {
		return map[string]any{}
	}
	out, _ := c.allFlags(ctx, distinctID, o)
	return out
}

// allFlags is GetAllFlags with payloads.
func (c *Client) allFlags(ctx context.Context, distinctID string, o FlagOptions) (values, payloads map[string]any) {
	values, payloads = map[string]any{}, map[string]any{}
	loc := c.local(distinctID, o)
	undecided := loc == nil
	for k, r := range loc {
		if r.Reason == ReasonRequiresServer {
			undecided = true
		} else {
			values[k] = r.Value
			if r.Payload != nil {
				payloads[k] = r.Payload
			}
		}
	}
	if undecided && !o.OnlyEvaluateLocally {
		for k, r := range c.remote(ctx, distinctID, o, nil) {
			if _, ok := values[k]; !ok {
				values[k] = r.value
				if r.payload != nil {
					payloads[k] = r.payload
				}
			}
		}
	}
	return values, payloads
}
