// Code copied from go/internal/flageval (the reference); edit there and re-sync.

package lucida

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

// matchV2 applies the spec v2 operators (string affixes, case-insensitive contains, ranges, dates,
// semver ranges) to a set property. Unknown operators never match.
func matchV2(op string, prop, want any, now time.Time) bool {
	switch op {
	case OpIContains, OpNotIContains, OpStartsWith, OpNotStartsWith, OpEndsWith, OpNotEndsWith:
		s, ok1 := prop.(string)
		w, ok2 := want.(string)
		if !ok1 || !ok2 {
			return false
		}
		switch op {
		case OpIContains, OpNotIContains:
			return strings.Contains(strings.ToLower(s), strings.ToLower(w)) == (op == OpIContains)
		case OpStartsWith, OpNotStartsWith:
			return strings.HasPrefix(s, w) == (op == OpStartsWith)
		}
		return strings.HasSuffix(s, w) == (op == OpEndsWith)
	case OpBetween, OpNotBetween:
		list, ok := want.([]any)
		x, okx := toFloat(prop)
		if !ok || len(list) != 2 || !okx || !isNum(list[0]) || !isNum(list[1]) {
			return false
		}
		lo, _ := toFloat(list[0])
		hi, _ := toFloat(list[1])
		return (x >= lo && x <= hi) == (op == OpBetween)
	case OpDateBefore, OpDateAfter:
		p, ok1 := parseInstant(prop, now, false)
		w, ok2 := parseInstant(want, now, true)
		if !ok1 || !ok2 {
			return false
		}
		if op == OpDateBefore {
			return p.Before(w)
		}
		return p.After(w)
	case OpSemverEq, OpSemverNeq, OpSemverGt, OpSemverGte, OpSemverLt, OpSemverLte:
		c, ok := cmpSemver(prop, want)
		if !ok {
			return false
		}
		switch op {
		case OpSemverEq:
			return c == 0
		case OpSemverNeq:
			return c != 0
		case OpSemverGt:
			return c > 0
		case OpSemverGte:
			return c >= 0
		case OpSemverLt:
			return c < 0
		}
		return c <= 0
	case OpSemverTilde, OpSemverCaret, OpSemverWild:
		v, ok1 := parseSemver(prop)
		lo, hi, ok2 := SemverRange(op, want)
		return ok1 && ok2 && cmpParts(v, lo) >= 0 && cmpParts(v, hi) < 0
	}
	return false
}

// SemverRange returns the half-open range [lo, hi) of a tilde, caret or wildcard value:
// ~1.2.3 -> [1.2.3, 1.3.0), ~1 -> [1.0.0, 2.0.0); ^1.2.3 -> [1.2.3, 2.0.0), ^0.2.3 -> [0.2.3, 0.3.0),
// ^0.0.3 -> [0.0.3, 0.0.4); 1.2.* (or .x) -> [1.2, 1.3), * -> every version.
func SemverRange(op string, want any) (lo, hi []int, ok bool) {
	s, isStr := want.(string)
	if !isStr {
		return nil, nil, false
	}
	parts := strings.Split(strings.TrimPrefix(strings.TrimSpace(s), "v"), ".")
	if op == OpSemverWild {
		last := parts[len(parts)-1]
		if last != "*" && last != "x" && last != "X" {
			return nil, nil, false
		}
		if len(parts) == 1 {
			return []int{0}, []int{1 << 30}, true
		}
		p, ok := parseSemver(strings.Join(parts[:len(parts)-1], "."))
		if !ok {
			return nil, nil, false
		}
		hi = append([]int(nil), p...)
		hi[len(hi)-1]++
		return p, hi, true
	}
	p, ok := parseSemver(s)
	if !ok {
		return nil, nil, false
	}
	for len(p) < 3 {
		p = append(p, 0)
	}
	switch {
	case op == OpSemverTilde && len(parts) == 1:
		hi = []int{p[0] + 1, 0, 0}
	case op == OpSemverTilde, p[0] == 0 && p[1] > 0:
		hi = []int{p[0], p[1] + 1, 0}
	case p[0] > 0:
		hi = []int{p[0] + 1, 0, 0}
	default:
		hi = []int{0, 0, p[2] + 1}
	}
	return p, hi, true
}

func cmpParts(a, b []int) int {
	for i := 0; i < max(len(a), len(b)); i++ {
		var x, y int
		if i < len(a) {
			x = a[i]
		}
		if i < len(b) {
			y = b[i]
		}
		if x != y {
			return cmpF(float64(x), float64(y))
		}
	}
	return 0
}

var relDate = regexp.MustCompile(`^-?(\d{1,6})([hdwmy])$`)

// RelativeSeconds maps a relative date value ("-7d", "3h", "-2w", "-1m", "-1y") to seconds before
// now. Units are fixed lengths (m = 30 days, y = 365 days) so every evaluator agrees.
func RelativeSeconds(s string) (int64, bool) {
	m := relDate.FindStringSubmatch(strings.TrimSpace(s))
	if m == nil {
		return 0, false
	}
	n, _ := strconv.ParseInt(m[1], 10, 64)
	unit := map[string]int64{"h": 3600, "d": 86400, "w": 7 * 86400, "m": 30 * 86400, "y": 365 * 86400}[m[2]]
	return n * unit, true
}

// parseInstant reads a number (unix seconds), an RFC 3339 timestamp or a YYYY-MM-DD date (UTC
// midnight); a filter value (rel) may also be a relative date counted back from now.
func parseInstant(v any, now time.Time, rel bool) (time.Time, bool) {
	if isNum(v) {
		f, _ := toFloat(v)
		return time.Unix(0, int64(f*1e9)).UTC(), true
	}
	s, ok := v.(string)
	if !ok {
		return time.Time{}, false
	}
	s = strings.TrimSpace(s)
	if rel {
		if secs, ok := RelativeSeconds(s); ok {
			return now.Add(-time.Duration(secs) * time.Second), true
		}
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, true
	}
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t, true
	}
	return time.Time{}, false
}

// now is the evaluation clock: EvalContext.Now when it parses, else the wall clock.
func (c EvalContext) now() time.Time {
	if t, err := time.Parse(time.RFC3339Nano, c.Now); err == nil {
		return t
	}
	return time.Now()
}
