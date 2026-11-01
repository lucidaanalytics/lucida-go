// Code copied from go/internal/flageval (the reference); edit there and re-sync.

package lucida

func off(reason string, idx int) FlagResult {
	return FlagResult{Value: false, Reason: reason, ConditionIndex: idx}
}

// BucketingKey returns the hash unit of f for c, or "" when unavailable (sticky stores key on it).
func BucketingKey(f Flag, c EvalContext) string { return bucketingKey(f, c) }

// bucketingKey returns the hash unit for f, or "" when unavailable.
func bucketingKey(f Flag, c EvalContext) string { return unitKey(f.Aggregation, c) }

// unitKey returns the hash unit an aggregation selects from c, or "" when absent.
func unitKey(a Aggregation, c EvalContext) string {
	switch a.Kind {
	case "group":
		return c.Groups[a.GroupType]
	case "device":
		return c.DeviceID
	case "context":
		e, _ := contextEntry(c, a.ContextKind)
		return e.Key
	}
	if c.BucketingKey != "" {
		return c.BucketingKey
	}
	return c.DistinctID
}

// evaluate evaluates one flag. Prerequisites are read from c.Resolved; use
// EvaluateAll to resolve them automatically. A condition matches when all its
// filters pass AND the rollout hash is under rollout_pct; otherwise the next
// condition is tried (unless early_exit). A condition with its own aggregation buckets on that
// unit and is skipped when the unit is absent.
func evaluate(f Flag, c EvalContext) FlagResult {
	if !appContextAllowed(f, c) {
		return off(ReasonAppContextExcluded, -1)
	}
	if !f.Active {
		return off(ReasonInactive, -1)
	}
	if f.KillSwitch {
		return off(ReasonKillSwitch, -1)
	}
	server := false
	for _, p := range f.Prerequisites {
		r, ok := c.Resolved[p.Flag]
		if !ok {
			return off(ReasonPrerequisiteMissing, -1)
		}
		switch {
		case r.Reason == ReasonRequiresServer:
			server = true // undecidable locally; a definite failure elsewhere still wins
		case r.Reason == ReasonPrerequisiteCycle || !valueMatches(r.Value, p.Value):
			return off(ReasonPrerequisiteFailed, -1)
		}
	}
	if server {
		return off(ReasonRequiresServer, -1)
	}
	key := bucketingKey(f, c)
	if key == "" && (!perConditionUnits(f) || f.Holdout != nil || f.Namespace != nil) {
		return off(ReasonNoBucketingKey, -1)
	}
	if f.Holdout != nil && rolloutHash("holdout", f.Holdout.Salt, key)*100 < f.Holdout.Percentage {
		return off(ReasonHoldout, -1)
	}
	if ns := f.Namespace; ns != nil && ns.Total > 0 {
		b := int(rolloutHash("namespace", ns.Salt, key) * float64(ns.Total))
		if b < ns.Start || b >= ns.End {
			return off(ReasonNamespaceExcluded, -1)
		}
	}
	skipped := 0
	for i, cond := range f.Conditions {
		ckey := key
		if cond.Aggregation != nil {
			ckey = unitKey(*cond.Aggregation, c)
		}
		if ckey == "" {
			skipped++
			continue
		}
		ok, server := conditionMatches(cond, c)
		if server {
			return off(ReasonRequiresServer, i)
		}
		if !ok {
			continue
		}
		if cond.Aggregation == nil { // the sticky store keys on the flag-level unit
			if r, hit := stickyResult(f, cond, i, c); hit {
				return r
			}
		}
		if rolloutHash(f.Key, f.Salt, ckey)*100 < cond.RolloutPct {
			return decide(f, cond, i, ckey)
		}
		if f.EarlyExit {
			return off(ReasonOutOfRollout, i)
		}
	}
	if skipped > 0 && skipped == len(f.Conditions) {
		return off(ReasonNoBucketingKey, -1)
	}
	return off(ReasonNoConditionMatch, -1)
}

func perConditionUnits(f Flag) bool {
	for _, cond := range f.Conditions {
		if cond.Aggregation != nil {
			return true
		}
	}
	return false
}

// appContextAllowed: a flag with app contexts evaluates only for a request declaring one of them;
// a request declaring none is not filtered.
func appContextAllowed(f Flag, c EvalContext) bool {
	if len(f.AppContexts) == 0 || len(c.AppContexts) == 0 {
		return true
	}
	for _, want := range f.AppContexts {
		for _, have := range c.AppContexts {
			if want == have {
				return true
			}
		}
	}
	return false
}

// valueMatches: want==true matches any on-value (true or a variant); else equality.
func valueMatches(got, want any) bool {
	if b, ok := want.(bool); ok {
		if gb, ok := got.(bool); ok {
			return gb == b
		}
		return b // got is a variant string => "on"
	}
	return got == want
}

func decide(f Flag, cond Condition, idx int, key string) FlagResult {
	res := FlagResult{Value: true, Reason: ReasonConditionMatch, ConditionIndex: idx}
	if f.Type == TypeMultivariate && len(f.Variants) > 0 {
		v := pickVariant(f, cond, key)
		res.Value, res.Variant, res.Payload = v.Key, v.Key, v.Payload
	} else if f.Type == TypeJSON {
		res.Payload = f.Payload
	}
	return res
}

func pickVariant(f Flag, cond Condition, key string) Variant {
	if cond.VariantOverride != "" {
		for _, v := range f.Variants {
			if v.Key == cond.VariantOverride {
				return v
			}
		}
	}
	h := variantHash(f.Key, f.Salt, key) * 100
	cum := 0.0
	for _, v := range f.Variants {
		cum += v.RolloutPct
		if h < cum {
			return v
		}
	}
	return f.Variants[len(f.Variants)-1] // percentages summed < 100: last variant absorbs the remainder
}

// conditionMatches ANDs the filters. Non-cohort failures win over cohort (and requires_server flag) filters,
// so server is true only when every locally evaluable filter passed.
func conditionMatches(cond Condition, c EvalContext) (ok, server bool) {
	hasCohort := false
	for _, fl := range cond.Filters {
		if fl.Type == FilterCohort {
			member, known := c.Cohorts[fl.Key]
			if !known {
				hasCohort = true
				continue
			}
			if member == (fl.Op == OpNotIn || fl.Op == OpNeq) {
				return false, false
			}
			continue
		}
		if fl.Type == FilterFlag && c.Resolved[fl.Key].Reason == ReasonRequiresServer {
			hasCohort = true // undecidable locally: propagate like a cohort filter
			continue
		}
		if !filterMatches(fl, c) {
			return false, false
		}
	}
	return !hasCohort, hasCohort
}

func filterMatches(fl Filter, c EvalContext) bool {
	now := c.now()
	var props map[string]any
	switch fl.Type {
	case FilterPerson:
		props = c.PersonProperties
	case FilterGroup:
		props = c.GroupProperties[fl.GroupType]
	case FilterDevice:
		props = c.DeviceProperties
	case FilterGeo:
		props = c.GeoProperties
	case FilterAppVersion:
		return match(fl.Op, c.AppVersion, c.AppVersion != "", fl.Value, true, now)
	case FilterContext:
		e, ok := contextEntry(c, fl.ContextKind)
		if !ok {
			return false
		}
		v, present := e.Properties[fl.Key]
		return match(fl.Op, v, present, fl.Value, false, now)
	case FilterFlag:
		r, ok := c.Resolved[fl.Key]
		return ok && match(fl.Op, r.Value, true, fl.Value, false, now)
	default:
		return false
	}
	v, present := props[fl.Key]
	return match(fl.Op, v, present, fl.Value, false, now)
}
