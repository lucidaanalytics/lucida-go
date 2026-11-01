// Package lucidaprovider is an OpenFeature (Apache-2.0) server provider over the Lucida Go SDK.
// The targeting key is the Lucida distinct id; every other context attribute becomes a person property.
// Variants resolve to the string value, payload flags to Object evaluations.
package lucidaprovider

import (
	"context"
	"math"

	"github.com/open-feature/go-sdk/openfeature"

	lucida "github.com/lucidaanalytics/lucida-go"
)

// Flags is the slice of *lucida.Client the provider needs.
type Flags interface {
	GetFeatureFlag(ctx context.Context, key, distinctID string, o lucida.FlagOptions) (any, bool)
	GetFeatureFlagPayload(ctx context.Context, key, distinctID string, o lucida.FlagOptions) any
}

// Provider implements openfeature.FeatureProvider.
type Provider struct{ flags Flags }

var _ openfeature.FeatureProvider = (*Provider)(nil)

// New wraps a Lucida client (or any Flags).
func New(flags Flags) *Provider { return &Provider{flags: flags} }

func (p *Provider) Metadata() openfeature.Metadata { return openfeature.Metadata{Name: "lucida"} }
func (p *Provider) Hooks() []openfeature.Hook      { return nil }

func fail(err openfeature.ResolutionError) openfeature.ProviderResolutionDetail {
	return openfeature.ProviderResolutionDetail{ResolutionError: err, Reason: openfeature.ErrorReason}
}

func mismatch(flag, want string) openfeature.ProviderResolutionDetail {
	return fail(openfeature.NewTypeMismatchResolutionError("flag " + flag + " is not " + want))
}

// resolve returns the raw flag value, or false with the detail describing why there is none.
func (p *Provider) resolve(ctx context.Context, flag string, fc openfeature.FlattenedContext) (any, string, openfeature.ProviderResolutionDetail, bool) {
	id, _ := fc[openfeature.TargetingKey].(string)
	if id == "" {
		return nil, "", fail(openfeature.NewTargetingKeyMissingResolutionError("targetingKey (distinct id) is required")), false
	}
	props := map[string]any{}
	for k, v := range fc {
		if k != openfeature.TargetingKey {
			props[k] = v
		}
	}
	v, ok := p.flags.GetFeatureFlag(ctx, flag, id, lucida.FlagOptions{PersonProperties: props})
	if !ok {
		return nil, id, fail(openfeature.NewFlagNotFoundResolutionError("flag " + flag + " not found")), false
	}
	d := openfeature.ProviderResolutionDetail{Reason: openfeature.TargetingMatchReason}
	if s, isStr := v.(string); isStr {
		d.Variant = s
	}
	return v, id, d, true
}

func (p *Provider) BooleanEvaluation(ctx context.Context, flag string, def bool, fc openfeature.FlattenedContext) openfeature.BoolResolutionDetail {
	v, _, d, ok := p.resolve(ctx, flag, fc)
	if !ok {
		return openfeature.BoolResolutionDetail{Value: def, ProviderResolutionDetail: d}
	}
	switch x := v.(type) {
	case bool:
		return openfeature.BoolResolutionDetail{Value: x, ProviderResolutionDetail: d}
	case string: // a multivariate flag is "on" for any variant, mirroring isFeatureEnabled
		return openfeature.BoolResolutionDetail{Value: true, ProviderResolutionDetail: d}
	}
	return openfeature.BoolResolutionDetail{Value: def, ProviderResolutionDetail: mismatch(flag, "boolean")}
}

func (p *Provider) StringEvaluation(ctx context.Context, flag string, def string, fc openfeature.FlattenedContext) openfeature.StringResolutionDetail {
	v, _, d, ok := p.resolve(ctx, flag, fc)
	if !ok {
		return openfeature.StringResolutionDetail{Value: def, ProviderResolutionDetail: d}
	}
	s, isStr := v.(string)
	if !isStr {
		return openfeature.StringResolutionDetail{Value: def, ProviderResolutionDetail: mismatch(flag, "a string variant")}
	}
	return openfeature.StringResolutionDetail{Value: s, ProviderResolutionDetail: d}
}

// payload resolves the flag then reads its payload (without a second exposure event).
func (p *Provider) payload(ctx context.Context, flag string, fc openfeature.FlattenedContext) (any, openfeature.ProviderResolutionDetail, bool) {
	_, id, d, ok := p.resolve(ctx, flag, fc)
	if !ok {
		return nil, d, false
	}
	return p.flags.GetFeatureFlagPayload(ctx, flag, id, lucida.FlagOptions{NoExposure: true}), d, true
}

func (p *Provider) FloatEvaluation(ctx context.Context, flag string, def float64, fc openfeature.FlattenedContext) openfeature.FloatResolutionDetail {
	pl, d, ok := p.payload(ctx, flag, fc)
	if !ok {
		return openfeature.FloatResolutionDetail{Value: def, ProviderResolutionDetail: d}
	}
	f, isNum := pl.(float64)
	if !isNum {
		return openfeature.FloatResolutionDetail{Value: def, ProviderResolutionDetail: mismatch(flag, "a numeric payload")}
	}
	return openfeature.FloatResolutionDetail{Value: f, ProviderResolutionDetail: d}
}

func (p *Provider) IntEvaluation(ctx context.Context, flag string, def int64, fc openfeature.FlattenedContext) openfeature.IntResolutionDetail {
	pl, d, ok := p.payload(ctx, flag, fc)
	if !ok {
		return openfeature.IntResolutionDetail{Value: def, ProviderResolutionDetail: d}
	}
	f, isNum := pl.(float64)
	if !isNum || f != math.Trunc(f) {
		return openfeature.IntResolutionDetail{Value: def, ProviderResolutionDetail: mismatch(flag, "an integer payload")}
	}
	return openfeature.IntResolutionDetail{Value: int64(f), ProviderResolutionDetail: d}
}

func (p *Provider) ObjectEvaluation(ctx context.Context, flag string, def any, fc openfeature.FlattenedContext) openfeature.InterfaceResolutionDetail {
	pl, d, ok := p.payload(ctx, flag, fc)
	if !ok {
		return openfeature.InterfaceResolutionDetail{Value: def, ProviderResolutionDetail: d}
	}
	if pl == nil {
		return openfeature.InterfaceResolutionDetail{Value: def, ProviderResolutionDetail: mismatch(flag, "an object payload")}
	}
	return openfeature.InterfaceResolutionDetail{Value: pl, ProviderResolutionDetail: d}
}
