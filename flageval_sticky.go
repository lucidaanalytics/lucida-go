// Code copied from go/internal/flageval (the reference); edit there and re-sync.

package lucida

// stickyResult serves the unit's stored assignment for a sticky flag once condition cond (index i)
// passed its filters: the rollout hash and variant weights are skipped, so the unit keeps its value
// when allocation changes. Only on-values are sticky; a value the flag can no longer serve (unknown
// variant, wrong type) is ignored, and a variant_override naming a variant wins over the store.
func stickyResult(f Flag, cond Condition, i int, c EvalContext) (FlagResult, bool) {
	stored, ok := c.StickyAssignments[f.Key]
	if !f.Sticky || !ok {
		return FlagResult{}, false
	}
	res := FlagResult{Value: true, Reason: ReasonStickyBucket, ConditionIndex: i}
	if f.Type == TypeMultivariate && len(f.Variants) > 0 {
		key, _ := stored.(string)
		var hit *Variant
		for j, v := range f.Variants {
			if v.Key == cond.VariantOverride {
				return FlagResult{}, false
			}
			if v.Key == key {
				hit = &f.Variants[j]
			}
		}
		if hit == nil {
			return FlagResult{}, false
		}
		res.Value, res.Variant, res.Payload = hit.Key, hit.Key, hit.Payload
		return res, true
	}
	if stored != true {
		return FlagResult{}, false
	}
	if f.Type == TypeJSON {
		res.Payload = f.Payload
	}
	return res, true
}

// StickyValue is what a sticky store should persist for result r of flag f: the on-value of a fresh
// hash-based match. ok is false when nothing should be stored (not sticky, off, or already sticky).
func StickyValue(f Flag, r FlagResult) (any, bool) {
	if !f.Sticky || r.Reason != ReasonConditionMatch || r.Value == false {
		return nil, false
	}
	return r.Value, true
}
