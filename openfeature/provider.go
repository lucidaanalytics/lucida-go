// Package lucidaprovider is an OpenFeature (Apache-2.0) server provider over the Lucida Go SDK.
// The targeting key is the Lucida distinct id; the `groups` and `groupProperties` attributes map to Lucida
// groups, every other attribute becomes a person property. Variants resolve to the string value, payload
// flags to Object evaluations. Reasons follow testdata/openfeature/reasons.json; flagMetadata carries
// lucidaReason, conditionIndex, requestId and evaluatedLocally.
package lucidaprovider

import (
	"context"
	"math"

	"github.com/open-feature/go-sdk/openfeature"

	lucida "github.com/lucidaanalytics/lucida-go"
)

// Flags is the slice of *lucida.Client the provider needs.
type Flags interface {
	GetFeatureFlagDetails(ctx context.Context, key, distinctID string, o lucida.FlagOptions) (lucida.FlagDetails, bool)
	Capture(in lucida.CaptureInput)
	OnFlagsChange(changed func(), stale func(error))
}

// Provider implements openfeature.FeatureProvider, StateHandler, EventHandler and Tracker.
type Provider struct {
	flags Flags
	ev    events
}

var (
	_ openfeature.FeatureProvider = (*Provider)(nil)
	_ openfeature.StateHandler    = (*Provider)(nil)
	_ openfeature.EventHandler    = (*Provider)(nil)
	_ openfeature.Tracker         = (*Provider)(nil)
)

// New wraps a Lucida client (or any Flags) and subscribes to its definition reloads.
func New(flags Flags) *Provider {
	p := &Provider{flags: flags, ev: newEvents()}
	flags.OnFlagsChange(p.ev.changed, p.ev.stale)
	return p
}

func (p *Provider) Metadata() openfeature.Metadata { return openfeature.Metadata{Name: "lucida"} }
func (p *Provider) Hooks() []openfeature.Hook      { return nil }

// reasonTable mirrors testdata/openfeature/reasons.json (the test compares them).
var reasonTable = map[string]openfeature.Reason{
	"condition_match": openfeature.TargetingMatchReason, "sticky_bucket": openfeature.CachedReason,
	"no_condition_match": openfeature.DefaultReason, "out_of_rollout": openfeature.DefaultReason,
	"holdout": openfeature.DefaultReason, "namespace_excluded": openfeature.DefaultReason,
	"no_bucketing_key": openfeature.DefaultReason, "prerequisite_failed": openfeature.DefaultReason,
	"prerequisite_missing": openfeature.DefaultReason, "prerequisite_cycle": openfeature.ErrorReason,
	"inactive": openfeature.DisabledReason, "kill_switch": openfeature.DisabledReason,
	"app_context_excluded": openfeature.DisabledReason, "preview": openfeature.StaticReason,
	"user_preview": openfeature.StaticReason, "fallback": openfeature.DefaultReason,
}

func ofReason(raw string, value any) openfeature.Reason {
	r, ok := reasonTable[raw]
	if !ok {
		return openfeature.UnknownReason
	}
	if _, isStr := value.(string); isStr && raw == "condition_match" {
		return openfeature.SplitReason
	}
	return r
}

func fail(err openfeature.ResolutionError) openfeature.ProviderResolutionDetail {
	return openfeature.ProviderResolutionDetail{ResolutionError: err, Reason: openfeature.ErrorReason}
}

func mismatch(flag, want string) openfeature.ProviderResolutionDetail {
	return fail(openfeature.NewTypeMismatchResolutionError("flag " + flag + " is not " + want))
}

func metadata(d lucida.FlagDetails) openfeature.FlagMetadata {
	m := openfeature.FlagMetadata{"lucidaReason": d.Reason, "evaluatedLocally": d.EvaluatedLocally}
	if d.ConditionIndex >= 0 {
		m["conditionIndex"] = d.ConditionIndex
	}
	if d.RequestID != "" {
		m["requestId"] = d.RequestID
	}
	return m
}

// resolve returns the Lucida details, or false with the detail describing why there are none.
func (p *Provider) resolve(ctx context.Context, flag string, fc openfeature.FlattenedContext) (lucida.FlagDetails, openfeature.ProviderResolutionDetail, bool) {
	id, o := splitContext(fc)
	if id == "" {
		return lucida.FlagDetails{}, fail(openfeature.NewTargetingKeyMissingResolutionError("targetingKey (distinct id) is required")), false
	}
	d, ok := p.flags.GetFeatureFlagDetails(ctx, flag, id, o)
	if !ok {
		return d, fail(openfeature.NewFlagNotFoundResolutionError("flag " + flag + " not found")), false
	}
	return d, openfeature.ProviderResolutionDetail{Reason: ofReason(d.Reason, d.Value), Variant: d.Variant, FlagMetadata: metadata(d)}, true
}

func (p *Provider) BooleanEvaluation(ctx context.Context, flag string, def bool, fc openfeature.FlattenedContext) openfeature.BoolResolutionDetail {
	d, pd, ok := p.resolve(ctx, flag, fc)
	if !ok {
		return openfeature.BoolResolutionDetail{Value: def, ProviderResolutionDetail: pd}
	}
	switch x := d.Value.(type) {
	case bool:
		return openfeature.BoolResolutionDetail{Value: x, ProviderResolutionDetail: pd}
	case string: // a multivariate flag is "on" for any variant, mirroring isFeatureEnabled
		return openfeature.BoolResolutionDetail{Value: true, ProviderResolutionDetail: pd}
	}
	return openfeature.BoolResolutionDetail{Value: def, ProviderResolutionDetail: mismatch(flag, "boolean")}
}

func (p *Provider) StringEvaluation(ctx context.Context, flag string, def string, fc openfeature.FlattenedContext) openfeature.StringResolutionDetail {
	d, pd, ok := p.resolve(ctx, flag, fc)
	if !ok {
		return openfeature.StringResolutionDetail{Value: def, ProviderResolutionDetail: pd}
	}
	s, isStr := d.Value.(string)
	if !isStr {
		return openfeature.StringResolutionDetail{Value: def, ProviderResolutionDetail: mismatch(flag, "a string variant")}
	}
	return openfeature.StringResolutionDetail{Value: s, ProviderResolutionDetail: pd}
}

// payload is the flag's payload; a fallback has none, so its value stands in.
func payload(d lucida.FlagDetails) any {
	if d.Reason == lucida.ReasonFallback {
		return d.Value
	}
	return d.Payload
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	}
	return 0, false
}

func (p *Provider) FloatEvaluation(ctx context.Context, flag string, def float64, fc openfeature.FlattenedContext) openfeature.FloatResolutionDetail {
	d, pd, ok := p.resolve(ctx, flag, fc)
	if !ok {
		return openfeature.FloatResolutionDetail{Value: def, ProviderResolutionDetail: pd}
	}
	f, isNum := toFloat(payload(d))
	if !isNum {
		return openfeature.FloatResolutionDetail{Value: def, ProviderResolutionDetail: mismatch(flag, "a numeric payload")}
	}
	return openfeature.FloatResolutionDetail{Value: f, ProviderResolutionDetail: pd}
}

func (p *Provider) IntEvaluation(ctx context.Context, flag string, def int64, fc openfeature.FlattenedContext) openfeature.IntResolutionDetail {
	d, pd, ok := p.resolve(ctx, flag, fc)
	if !ok {
		return openfeature.IntResolutionDetail{Value: def, ProviderResolutionDetail: pd}
	}
	f, isNum := toFloat(payload(d))
	if !isNum || f != math.Trunc(f) {
		return openfeature.IntResolutionDetail{Value: def, ProviderResolutionDetail: mismatch(flag, "an integer payload")}
	}
	return openfeature.IntResolutionDetail{Value: int64(f), ProviderResolutionDetail: pd}
}

func (p *Provider) ObjectEvaluation(ctx context.Context, flag string, def any, fc openfeature.FlattenedContext) openfeature.InterfaceResolutionDetail {
	d, pd, ok := p.resolve(ctx, flag, fc)
	if !ok {
		return openfeature.InterfaceResolutionDetail{Value: def, ProviderResolutionDetail: pd}
	}
	pl := payload(d)
	if pl == nil {
		return openfeature.InterfaceResolutionDetail{Value: def, ProviderResolutionDetail: mismatch(flag, "an object payload")}
	}
	return openfeature.InterfaceResolutionDetail{Value: pl, ProviderResolutionDetail: pd}
}
