package lucida

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"slices"
	"testing"
	"time"
)

var testdata = func() string {
	if d := os.Getenv("LUCIDA_TESTDATA"); d != "" {
		return d + "/"
	}
	return "../../testdata/"
}()

func loadJSON(t *testing.T, rel string, v any) {
	t.Helper()
	raw, err := os.ReadFile(testdata + rel)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, v); err != nil {
		t.Fatal(err)
	}
}

// supplied keeps only the context fields named by the vector's `needs`: anything else must not matter.
func supplied(c EvalContext, needs []string) EvalContext {
	if needs == nil {
		return c
	}
	var out EvalContext
	has := func(n string) bool { return slices.Contains(needs, n) }
	if has("distinct_id") {
		out.DistinctID, out.BucketingKey = c.DistinctID, c.BucketingKey
	}
	if has("groups") {
		out.Groups = c.Groups
	}
	if has("person_properties") {
		out.PersonProperties = c.PersonProperties
	}
	if has("group_properties") {
		out.GroupProperties = c.GroupProperties
	}
	if has("device_properties") {
		out.DeviceProperties = c.DeviceProperties
	}
	if has("geo_properties") {
		out.GeoProperties = c.GeoProperties
	}
	if has("app_version") {
		out.AppVersion = c.AppVersion
	}
	if has("device_id") {
		out.DeviceID = c.DeviceID
	}
	if has("contexts") {
		out.Contexts = c.Contexts
	}
	out.StickyAssignments, out.AppContexts, out.Now = c.StickyAssignments, c.AppContexts, c.Now
	return out
}

func runVectors(t *testing.T, file string, min int) {
	var d struct {
		HashVectors []struct {
			Input string  `json:"input"`
			Hash  float64 `json:"hash"`
		} `json:"hash_vectors"`
		Flags   []Flag `json:"flags"`
		Vectors []struct {
			Name     string      `json:"name"`
			Flag     string      `json:"flag"`
			Needs    []string    `json:"needs"`
			Context  EvalContext `json:"context"`
			Expected struct {
				Value          any    `json:"value"`
				Variant        string `json:"variant"`
				Payload        any    `json:"payload"`
				Reason         string `json:"reason"`
				ConditionIndex int    `json:"condition_index"`
			} `json:"expected"`
		} `json:"vectors"`
	}
	loadJSON(t, "flags/"+file, &d)
	for _, h := range d.HashVectors {
		if Hash(h.Input) != h.Hash {
			t.Fatalf("hash(%q) = %v, want %v", h.Input, Hash(h.Input), h.Hash)
		}
	}
	if len(d.Vectors) < min {
		t.Fatalf("only %d vectors", len(d.Vectors))
	}
	for _, v := range d.Vectors {
		got := EvaluateAll(d.Flags, supplied(v.Context, v.Needs))[v.Flag]
		e := v.Expected
		if !reflect.DeepEqual(got.Value, e.Value) || got.Variant != e.Variant || !reflect.DeepEqual(got.Payload, e.Payload) ||
			got.Reason != e.Reason || got.ConditionIndex != e.ConditionIndex {
			t.Fatalf("%s: got %+v, want %+v", v.Name, got, e)
		}
	}
}

func TestVectorsBasic(t *testing.T)     { runVectors(t, "vectors_basic.json", 20) }
func TestVectorsG10(t *testing.T)       { runVectors(t, "vectors_g10.json", 15) }
func TestVectorsSticky(t *testing.T)    { runVectors(t, "vectors_sticky.json", 19) }
func TestVectorsG18(t *testing.T)       { runVectors(t, "vectors_g18.json", 20) }
func TestVectorsOps(t *testing.T)       { runVectors(t, "vectors_ops.json", 110) }
func TestVectorsGenerated(t *testing.T) { runVectors(t, "vectors.json", 10_000) }

type fixtureCase struct {
	Name string `json:"name"`
	Call struct {
		Method     string            `json:"method"`
		DistinctID string            `json:"distinct_id"`
		Event      string            `json:"event"`
		Properties map[string]any    `json:"properties"`
		PropsOnce  map[string]any    `json:"properties_set_once"`
		Groups     map[string]string `json:"groups"`
		Set        map[string]any    `json:"set"`
		SetOnce    map[string]any    `json:"set_once"`
		Timestamp  string            `json:"timestamp"`
		UUID       string            `json:"uuid"`
		Alias      string            `json:"alias"`
		GroupType  string            `json:"group_type"`
		GroupKey   string            `json:"group_key"`
	} `json:"call"`
	Expected map[string]any `json:"expected"`
}

func TestEventFixtures(t *testing.T) {
	var f struct {
		Cases []fixtureCase `json:"cases"`
	}
	loadJSON(t, "events/server_sdk.json", &f)
	for _, tc := range f.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			srv := newFake(t)
			c := New("lc_pk_x", Config{Host: srv.URL, FlushInterval: time.Hour})
			k := tc.Call
			switch k.Method {
			case "capture":
				in := CaptureInput{DistinctID: k.DistinctID, Event: k.Event, Properties: k.Properties, Groups: k.Groups, UUID: k.UUID, Set: k.Set, SetOnce: k.SetOnce}
				if k.Timestamp != "" {
					in.Timestamp, _ = time.Parse(time.RFC3339Nano, k.Timestamp)
				}
				c.Capture(in)
			case "identify":
				c.Identify(k.DistinctID, k.Properties, k.PropsOnce)
			case "alias":
				c.Alias(k.DistinctID, k.Alias)
			default:
				c.GroupIdentify(k.GroupType, k.GroupKey, k.Properties, k.DistinctID)
			}
			if err := c.Shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
			evs := srv.events("/v1/batch")
			if len(evs) != 1 || srv.bodies("/v1/batch")[0]["api_key"] != "lc_pk_x" {
				t.Fatalf("want one event with api_key, got %v", evs)
			}
			ev := evs[0]
			props := ev["properties"].(map[string]any)
			if lib, _ := props["$lib"].(string); len(lib) < 7 || lib[:7] != "lucida-" || props["$lib_version"] == nil {
				t.Fatalf("missing $lib: %v", props)
			}
			delete(props, "$lib")
			delete(props, "$lib_version")
			want := tc.Expected
			for _, key := range []string{"uuid", "timestamp"} {
				if want[key] == "*" {
					if s, _ := ev[key].(string); s == "" {
						t.Fatalf("%s empty", key)
					}
					want[key] = ev[key]
				}
			}
			if !reflect.DeepEqual(ev, want) {
				t.Fatalf("got  %v\nwant %v", ev, want)
			}
		})
	}
}
