// Code copied from go/internal/flageval (the reference); edit there and re-sync.

package lucida

// Multi-kind contexts (LC-FLG-18). One evaluation can carry several contexts at once, one per
// kind (user, device, org). A `context` filter reads the properties of its kind and a flag with
// aggregation kind "context" buckets on that kind's key.

// EvalContext kinds.
const (
	KindUser   = "user"
	KindDevice = "device"
	KindOrg    = "org"
)

// ContextEntry is one kind's identity: the bucketing Key and its Properties.
type ContextEntry struct {
	Key        string         `json:"key"`
	Properties map[string]any `json:"properties,omitempty"`
}

// ValidContextKind reports whether kind is one of user, device or org.
func ValidContextKind(kind string) bool {
	return kind == KindUser || kind == KindDevice || kind == KindOrg
}

// contextEntry resolves the context of kind. An explicit Contexts[kind] wins. Without one, user
// falls back to the legacy fields (bucketing key or distinct id, person properties) and device to
// device_id and device properties, so single-kind callers keep working; org has no fallback.
// ok is false for an unknown kind.
func contextEntry(c EvalContext, kind string) (e ContextEntry, ok bool) {
	if !ValidContextKind(kind) {
		return ContextEntry{}, false
	}
	if e, found := c.Contexts[kind]; found {
		return e, true
	}
	switch kind {
	case KindUser:
		key := c.BucketingKey
		if key == "" {
			key = c.DistinctID
		}
		return ContextEntry{Key: key, Properties: c.PersonProperties}, true
	case KindDevice:
		return ContextEntry{Key: c.DeviceID, Properties: c.DeviceProperties}, true
	}
	return ContextEntry{}, true
}

// needsContext adds the EvalContext fields a read of kind may touch (see Needs).
func needsContext(set map[string]bool, kind string) {
	set["contexts"] = true
	switch kind {
	case KindUser:
		set["distinct_id"], set["person_properties"] = true, true
	case KindDevice:
		set["device_id"], set["device_properties"] = true, true
	}
}
