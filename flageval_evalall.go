// Code copied from go/internal/flageval (the reference); edit there and re-sync.

package lucida

// EvaluateAll evaluates every flag, resolving prerequisites (and `flag` filters)
// depth-first. Flags on, or depending on, a prerequisite cycle get reason
// prerequisite_cycle. The caller's EvalContext.Resolved is ignored.
func EvaluateAll(flags []Flag, c EvalContext) map[string]FlagResult {
	byKey := make(map[string]Flag, len(flags))
	for _, f := range flags {
		byKey[f.Key] = f
	}
	out := make(map[string]FlagResult, len(flags))
	state := map[string]int{} // 1 = visiting, 2 = done
	c.Resolved = out
	var visit func(key string)
	visit = func(key string) {
		f, ok := byKey[key]
		if !ok || state[key] != 0 {
			return
		}
		state[key] = 1
		cyc := false
		for _, dep := range deps(f) {
			visit(dep)
			if state[dep] == 1 || out[dep].Reason == ReasonPrerequisiteCycle {
				cyc = true
			}
		}
		if cyc {
			out[key] = off(ReasonPrerequisiteCycle, -1)
		} else {
			out[key] = evaluate(f, c)
		}
		state[key] = 2
	}
	for _, f := range flags {
		visit(f.Key)
	}
	return out
}

func deps(f Flag) []string {
	var d []string
	for _, p := range f.Prerequisites {
		d = append(d, p.Flag)
	}
	for _, cond := range f.Conditions {
		for _, fl := range cond.Filters {
			if fl.Type == FilterFlag {
				d = append(d, fl.Key)
			}
		}
	}
	return d
}
