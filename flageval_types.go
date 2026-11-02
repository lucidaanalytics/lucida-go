// Code copied from go/internal/flageval (the reference); edit there and re-sync.

package lucida

// Flag types.
const (
	TypeBoolean      = "boolean"
	TypeMultivariate = "multivariate"
	TypeJSON         = "json"
)

// Filter types.
const (
	FilterPerson     = "person"
	FilterGroup      = "group"
	FilterCohort     = "cohort"
	FilterDevice     = "device"
	FilterGeo        = "geo"
	FilterAppVersion = "app_version"
	FilterFlag       = "flag"    // prerequisite: Key is a flag key, Value the expected value
	FilterContext    = "context" // property of the multi-kind context named by ContextKind (see contexts.go)
)

// Filter operators.
const (
	OpEq        = "eq"
	OpNeq       = "neq"
	OpContains  = "contains"
	OpNContains = "ncontains"
	OpRegex     = "regex"
	OpNRegex    = "nregex"
	OpGt        = "gt"
	OpGte       = "gte"
	OpLt        = "lt"
	OpLte       = "lte"
	OpIsSet     = "is_set"
	OpIsNotSet  = "is_not_set"
	OpIn        = "in"
	OpNotIn     = "not_in"
	// Spec v2 operators (vectors_ops.json), shared with the query compiler.
	OpIContains     = "icontains"
	OpNotIContains  = "not_icontains"
	OpStartsWith    = "starts_with"
	OpNotStartsWith = "not_starts_with"
	OpEndsWith      = "ends_with"
	OpNotEndsWith   = "not_ends_with"
	OpBetween       = "between"
	OpNotBetween    = "not_between"
	OpDateBefore    = "is_date_before"
	OpDateAfter     = "is_date_after"
	OpSemverEq      = "semver_eq"
	OpSemverNeq     = "semver_neq"
	OpSemverGt      = "semver_gt"
	OpSemverGte     = "semver_gte"
	OpSemverLt      = "semver_lt"
	OpSemverLte     = "semver_lte"
	OpSemverTilde   = "semver_tilde"
	OpSemverCaret   = "semver_caret"
	OpSemverWild    = "semver_wildcard"
)

// Ops lists every filter operator, in wire spelling.
var Ops = []string{OpEq, OpNeq, OpContains, OpNContains, OpRegex, OpNRegex, OpGt, OpGte, OpLt, OpLte,
	OpIsSet, OpIsNotSet, OpIn, OpNotIn, OpIContains, OpNotIContains, OpStartsWith, OpNotStartsWith,
	OpEndsWith, OpNotEndsWith, OpBetween, OpNotBetween, OpDateBefore, OpDateAfter, OpSemverEq,
	OpSemverNeq, OpSemverGt, OpSemverGte, OpSemverLt, OpSemverLte, OpSemverTilde, OpSemverCaret, OpSemverWild}

// FlagResult reasons.
const (
	ReasonInactive            = "inactive"
	ReasonKillSwitch          = "kill_switch"
	ReasonPrerequisiteFailed  = "prerequisite_failed"
	ReasonPrerequisiteCycle   = "prerequisite_cycle"
	ReasonPrerequisiteMissing = "prerequisite_missing"
	ReasonConditionMatch      = "condition_match"
	ReasonNoConditionMatch    = "no_condition_match"
	ReasonRequiresServer      = "requires_server"
	ReasonNoBucketingKey      = "no_bucketing_key"
	ReasonHoldout             = "holdout"
	ReasonNamespaceExcluded   = "namespace_excluded"
	ReasonStickyBucket        = "sticky_bucket"        // a stored assignment decided (see sticky.go)
	ReasonOutOfRollout        = "out_of_rollout"       // early_exit: filters matched, rollout hash excluded
	ReasonFallback            = "fallback"             // served from Config.Fallbacks; nothing decided the flag
	ReasonAppContextExcluded  = "app_context_excluded" // flag's app_contexts do not include the request's
)

// Flag is a flag definition (JSON tags are the cross-language wire format).
type Flag struct {
	Key           string         `json:"key"`
	Type          string         `json:"type"`
	Active        bool           `json:"active"` // false = archived/disabled
	KillSwitch    bool           `json:"kill_switch,omitempty"`
	Salt          string         `json:"salt"`
	Variants      []Variant      `json:"variants,omitempty"`
	Conditions    []Condition    `json:"conditions"`
	Aggregation   Aggregation    `json:"aggregation"`
	Prerequisites []Prerequisite `json:"prerequisites,omitempty"`
	Payload       any            `json:"payload,omitempty"` // json flags: payload served when on
	Namespace     *Namespace     `json:"namespace,omitempty"`
	Holdout       *Holdout       `json:"holdout,omitempty"`
	// Sticky keeps a unit's stored on-assignment (EvalContext.StickyAssignments) when allocation changes.
	Sticky bool `json:"sticky,omitempty"`
	// EncryptedPayload is the sealed payload of an encrypted remote-config flag (LC-FLG-26). Evaluation
	// ignores it; servers hand it to public-key callers instead of the plaintext Payload.
	EncryptedPayload string `json:"encrypted_payload,omitempty"`
	// EarlyExit stops at the first condition whose filters match: a rollout miss there is final
	// (reason out_of_rollout) instead of falling through to the next condition.
	EarlyExit bool `json:"early_exit,omitempty"`
	// AppContexts limits evaluation to requests declaring one of these app contexts (max 50). Empty
	// = everywhere; a request declaring none is not filtered.
	AppContexts []string `json:"app_contexts,omitempty"`
	// EvaluationRuntime is "client", "server" or empty (both). Evaluation ignores it: servers hide
	// server-only flags from public-key requests and client-only flags from secret-key callers.
	EvaluationRuntime string `json:"evaluation_runtime,omitempty"`
}

// Runtime values for Flag.EvaluationRuntime.
const (
	RuntimeClient = "client"
	RuntimeServer = "server"
)

// ServedTo reports whether a flag is served to a caller holding a secret (server) or public
// (client) key.
func (f Flag) ServedTo(secret bool) bool {
	return f.EvaluationRuntime == "" || (f.EvaluationRuntime == RuntimeServer) == secret
}

// Namespace restricts a flag to a bucket range of a mutually exclusive layer: the unit's bucket is
// floor(Hash("namespace." + Salt + "." + key) * Total); outside [Start, End) the flag is off.
type Namespace struct {
	Salt  string `json:"salt"`
	Start int    `json:"start"`
	End   int    `json:"end"`
	Total int    `json:"total"`
}

// Holdout removes a fixed share of units: Hash("holdout." + Salt + "." + key) * 100 < Percentage.
type Holdout struct {
	Salt       string  `json:"salt"`
	Percentage float64 `json:"percentage"`
}

// Variant is one multivariate arm; RolloutPct values are percentages of the matched population.
type Variant struct {
	Key        string  `json:"key"`
	RolloutPct float64 `json:"rollout_pct"`
	Payload    any     `json:"payload,omitempty"`
}

// Condition is an AND of filters plus a rollout percentage; first matching group wins.
type Condition struct {
	Filters         []Filter `json:"filters,omitempty"`
	RolloutPct      float64  `json:"rollout_pct"`
	VariantOverride string   `json:"variant_override,omitempty"`
	// Aggregation overrides the flag's bucketing unit for this condition; when the unit is absent
	// the condition is skipped.
	Aggregation *Aggregation `json:"aggregation,omitempty"`
}

// Filter is a single property predicate.
type Filter struct {
	Type      string `json:"type"`
	Key       string `json:"key"`
	Op        string `json:"op"`
	Value     any    `json:"value,omitempty"`
	GroupType string `json:"group_type,omitempty"` // for type=group
	// ContextKind is user, device or org for type=context.
	ContextKind string `json:"context_kind,omitempty"`
}

// Aggregation selects the bucketing unit: "person" (distinct id / bucketing key), "group", "device"
// (EvalContext.DeviceID) or "context" (the key of EvalContext.Contexts[ContextKind]).
type Aggregation struct {
	Kind        string `json:"kind"`
	GroupType   string `json:"group_type,omitempty"`
	ContextKind string `json:"context_kind,omitempty"`
}

// Prerequisite requires another flag to evaluate to Value (true, or a variant key).
type Prerequisite struct {
	Flag  string `json:"flag"`
	Value any    `json:"value"`
}

// EvalContext is the evaluation input.
type EvalContext struct {
	DistinctID       string                    `json:"distinct_id"`
	BucketingKey     string                    `json:"bucketing_key,omitempty"` // overrides DistinctID for person aggregation
	Groups           map[string]string         `json:"groups,omitempty"`        // group type -> group key
	PersonProperties map[string]any            `json:"person_properties,omitempty"`
	GroupProperties  map[string]map[string]any `json:"group_properties,omitempty"` // by group type
	DeviceProperties map[string]any            `json:"device_properties,omitempty"`
	GeoProperties    map[string]any            `json:"geo_properties,omitempty"`
	AppVersion       string                    `json:"app_version,omitempty"`
	DeviceID         string                    `json:"device_id,omitempty"` // bucketing unit for device aggregation
	// Contexts carries one identity per kind (user, device, org) for context filters and aggregation.
	Contexts map[string]ContextEntry `json:"contexts,omitempty"`
	// StickyAssignments maps flag key -> the unit's stored value (true or a variant key) for sticky flags.
	StickyAssignments map[string]any `json:"sticky_assignments,omitempty"`
	// AppContexts are the app contexts the caller declares (see Flag.AppContexts).
	AppContexts []string `json:"app_contexts,omitempty"`
	// Now (RFC 3339) anchors relative date values; empty = the wall clock.
	Now string `json:"now,omitempty"`

	// Cohorts is the cohort-membership hook: cohort id -> the unit is a member. A cohort id that is
	// absent is unknown, and a filter on it stays undecidable (requires_server), as when nil.
	Cohorts map[string]bool `json:"-"`

	// Resolved holds results of already-evaluated flags for prerequisites.
	// EvaluateAll fills it; direct Evaluate callers may supply it.
	Resolved map[string]FlagResult `json:"-"`
}

// FlagResult is the evaluation outcome. Value is false, true or a variant key (string).
type FlagResult struct {
	Value          any    `json:"value"`
	Variant        string `json:"variant,omitempty"`
	Payload        any    `json:"payload,omitempty"`
	Reason         string `json:"reason"`
	ConditionIndex int    `json:"condition_index"` // -1 when no condition decided the result
}
