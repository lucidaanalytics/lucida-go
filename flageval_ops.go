// Code copied from go/internal/flageval (the reference); edit there and re-sync.

package lucida

import (
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var reCache sync.Map // pattern -> *regexp.Regexp (nil if invalid)

func compile(p string) *regexp.Regexp {
	if v, ok := reCache.Load(p); ok {
		return v.(*regexp.Regexp)
	}
	re, _ := regexp.Compile(p)
	reCache.Store(p, re)
	return re
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
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return f, err == nil
	}
	return 0, false
}

func isNum(v any) bool {
	switch v.(type) {
	case float64, float32, int, int64:
		return true
	}
	return false
}

// equal compares scalars; two numbers compare numerically, a number never equals a string.
func equal(a, b any) bool {
	if isNum(a) || isNum(b) {
		if !isNum(a) || !isNum(b) {
			return false
		}
		fa, _ := toFloat(a)
		fb, _ := toFloat(b)
		return fa == fb
	}
	return a == b
}

// match applies op to a property value. Missing/null properties match only is_not_set.
func match(op string, prop any, present bool, want any, semver bool, now time.Time) bool {
	set := present && prop != nil
	switch op {
	case OpIsSet:
		return set
	case OpIsNotSet:
		return !set
	}
	if !set {
		return false
	}
	switch op {
	case OpEq, OpNeq:
		eq := false
		if semver {
			c, ok := cmpSemver(prop, want)
			if !ok {
				return false
			}
			eq = c == 0
		} else {
			eq = equal(prop, want)
		}
		return eq == (op == OpEq)
	case OpContains, OpNContains, OpRegex, OpNRegex:
		s, ok1 := prop.(string)
		w, ok2 := want.(string)
		if !ok1 || !ok2 {
			return false
		}
		if op == OpContains || op == OpNContains {
			return strings.Contains(s, w) == (op == OpContains)
		}
		re := compile(w)
		return re != nil && re.MatchString(s) == (op == OpRegex)
	case OpGt, OpGte, OpLt, OpLte:
		var c int
		if semver {
			var ok bool
			if c, ok = cmpSemver(prop, want); !ok {
				return false
			}
		} else {
			a, ok1 := toFloat(prop)
			b, ok2 := toFloat(want)
			if !ok1 || !ok2 {
				return false
			}
			c = cmpF(a, b)
		}
		return (op == OpGt && c > 0) || (op == OpGte && c >= 0) || (op == OpLt && c < 0) || (op == OpLte && c <= 0)
	case OpIn, OpNotIn:
		list, ok := want.([]any)
		if !ok {
			return false
		}
		found := false
		for _, w := range list {
			if equal(prop, w) {
				found = true
				break
			}
		}
		return found == (op == OpIn)
	}
	return matchV2(op, prop, want, now)
}

func cmpF(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// cmpSemver compares dotted numeric versions (major.minor.patch...), ignoring a
// leading "v" and any "-prerelease"/"+build" suffix; missing segments are 0.
func cmpSemver(a, b any) (int, bool) {
	pa, ok1 := parseSemver(a)
	pb, ok2 := parseSemver(b)
	if !ok1 || !ok2 {
		return 0, false
	}
	for i := 0; i < max(len(pa), len(pb)); i++ {
		var x, y int
		if i < len(pa) {
			x = pa[i]
		}
		if i < len(pb) {
			y = pb[i]
		}
		if x != y {
			return cmpF(float64(x), float64(y)), true
		}
	}
	return 0, true
}

func parseSemver(v any) ([]int, bool) {
	s, ok := v.(string)
	if !ok {
		return nil, false
	}
	s = strings.TrimPrefix(strings.TrimSpace(s), "v")
	if i := strings.IndexAny(s, "-+"); i >= 0 {
		s = s[:i]
	}
	var out []int
	for _, p := range strings.Split(s, ".") {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return nil, false
		}
		out = append(out, n)
	}
	return out, true
}
