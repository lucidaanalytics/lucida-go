// Package lucida is the Lucida server SDK: batched event capture, local feature-flag
// evaluation (shared vectors in testdata/flags) and historical import. Standard library only.
//
// Name a distinct id per call, or carry one in a context (WithRequestContext + CaptureContext).
package lucida

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Version is reported as $lib_version.
const Version = "0.1.0"

const (
	libName      = "lucida-go"
	minGzipBytes = 1024
	defaultHost  = "https://app.lucida.dev"
)

// Config configures a Client. Zero values pick the defaults noted.
type Config struct {
	Host             string        // default https://app.lucida.dev
	FlushAt          int           // events per batch, default 20
	FlushInterval    time.Duration // default 5s
	MaxQueueSize     int           // default 1000; the oldest events are dropped beyond it
	MaxRetries       int           // default 5
	SecretKey        string        // lc_sk_...: enables local flag evaluation and ImportEvents
	FlagPollInterval time.Duration // default 30s
	Timeout          time.Duration // per request, default 10s
	// StickyStore, when set, keeps local evaluations of sticky flags stable (NewMemoryStickyStore).
	StickyStore StickyStore
	// DisableFlagStream turns off the SSE flag stream (interval polling only). With a SecretKey the
	// client reloads definitions on server push and polls only while the stream is down.
	DisableFlagStream bool
	StreamBackoffBase time.Duration // first reconnect delay, default 1s (tests shrink it)
	Disabled          bool          // no events, no network, no goroutines
	Debug             bool          // log through Logf (default log.Printf)
	Logf              func(format string, args ...any)
	HTTPClient        *http.Client
	// AppContexts this server declares; flags limited to other app contexts are not served.
	AppContexts []string
	// FlagDefinitionCache shares one definitions fetch between workers (InMemoryFlagDefinitionCache is the reference).
	FlagDefinitionCache FlagDefinitionCache

	MaxBatchSize       int           // events per request (<= 1000), independent of FlushAt; default 1000
	FlagTimeout        time.Duration // per flag request (definitions, /v1/flags); default Timeout
	FlagDedupCacheSize int           // $feature_flag_called dedup entries, default 10000
	DisableGeoip       bool          // send $geoip_disable on every event (per event: CaptureInput.DisableGeoip)
	// BeforeSend may edit an event in place and return it, or return nil to drop it. A panic keeps the original.
	BeforeSend func(e CapturedEvent) *CapturedEvent
	// OnError is called when a batch is given up on (non-retryable status or retries exhausted) and when
	// Capture is given no distinct id. It runs on the delivery goroutine; keep it quick.
	OnError func(err error, events []CapturedEvent)
	// Fallbacks maps flag key to the value served (reason ReasonFallback, no exposure event) when nothing
	// decides the flag: network error, cold start, quota limit or unknown key. Beats FlagOptions.DefaultValue.
	Fallbacks map[string]any
}

// CaptureInput is one event. Timestamp and UUID default to now / a fresh UUIDv7.
type CaptureInput struct {
	DistinctID string
	Event      string
	Properties map[string]any
	Groups     map[string]string
	Timestamp  time.Time
	UUID       string
	Set        map[string]any // person properties ($set)
	SetOnce    map[string]any // person properties ($set_once)
	// SendFeatureFlags attaches $feature/<key> and $active_feature_flags: evaluated locally when
	// definitions are loaded, else with one remote call (the event is then queued asynchronously).
	SendFeatureFlags bool
	// FlagOptions shape the SendFeatureFlags evaluation (Groups default to the event's groups).
	FlagOptions FlagOptions
	// Flags attaches exactly this snapshot's values (EvaluateFlags); no evaluation, no network.
	Flags *FlagSnapshot
	// Personless sends $process_person_profile=false: no person is created or updated, Set/SetOnce are dropped,
	// and an empty DistinctID gets a random UUID.
	Personless bool
	// DisableGeoip overrides Config.DisableGeoip for this event (nil = client default).
	DisableGeoip *bool
}

type wireEvent struct {
	UUID       string            `json:"uuid"`
	Event      string            `json:"event"`
	DistinctID string            `json:"distinct_id"`
	Timestamp  string            `json:"timestamp"`
	Properties map[string]any    `json:"properties"`
	Set        map[string]any    `json:"$set,omitempty"`
	SetOnce    map[string]any    `json:"$set_once,omitempty"`
	Groups     map[string]string `json:"$groups,omitempty"`
}

func newWire(in CaptureInput) wireEvent {
	props := make(map[string]any, len(in.Properties)+2)
	for k, v := range in.Properties {
		props[k] = v
	}
	props["$lib"], props["$lib_version"] = libName, Version
	if in.UUID == "" {
		in.UUID = uuidV7()
	}
	if in.Timestamp.IsZero() {
		in.Timestamp = time.Now()
	}
	return wireEvent{
		UUID: in.UUID, Event: in.Event, DistinctID: in.DistinctID,
		Timestamp:  in.Timestamp.UTC().Format("2006-01-02T15:04:05.000Z"),
		Properties: props, Set: in.Set, SetOnce: in.SetOnce, Groups: in.Groups,
	}
}

// Client is safe for concurrent use. Call Shutdown to flush on exit.
type Client struct {
	apiKey, secret, host string
	cfg                  Config
	http                 *http.Client
	queue                *queue
	sink                 *testSink // NewTestClient only
	logf                 func(string, ...any)

	flagMu   sync.RWMutex
	defs     []Flag
	haveDefs bool
	etag     string
	seen     *lruSet
	onChange func()
	onStale  func(error)

	identity identityState // super properties and identify dedupe (identity.go)

	ready     chan struct{}
	stopPoll  chan struct{}
	pollOnce  sync.Once
	readyOne  sync.Once
	streaming atomic.Bool
}

// New creates a client and starts its background goroutines (none when cfg.Disabled).
func New(apiKey string, cfg Config) *Client {
	cfg.Host = strings.TrimRight(orStr(cfg.Host, defaultHost), "/")
	cfg.FlushAt = orInt(cfg.FlushAt, 20)
	cfg.MaxQueueSize = orInt(cfg.MaxQueueSize, 1000)
	cfg.MaxRetries = orInt(cfg.MaxRetries, 5)
	cfg.FlushInterval = orDur(cfg.FlushInterval, 5*time.Second)
	cfg.FlagPollInterval = orDur(cfg.FlagPollInterval, 30*time.Second)
	cfg.Timeout = orDur(cfg.Timeout, 10*time.Second)
	cfg.FlagTimeout = orDur(cfg.FlagTimeout, cfg.Timeout)
	cfg.MaxBatchSize = orInt(cfg.MaxBatchSize, maxBatch)
	c := &Client{
		apiKey: apiKey, secret: cfg.SecretKey, host: cfg.Host, cfg: cfg, http: cfg.HTTPClient,
		seen: newLRUSet(orInt(cfg.FlagDedupCacheSize, 10_000)), ready: make(chan struct{}), stopPoll: make(chan struct{}),
		logf: func(string, ...any) {},
	}
	if c.http == nil {
		c.http = &http.Client{}
	}
	if cfg.Debug {
		c.logf = cfg.Logf
		if c.logf == nil {
			c.logf = func(f string, a ...any) { log.Printf("[lucida] "+f, a...) }
		}
	}
	if cfg.Disabled {
		close(c.ready)
		return c
	}
	c.queue = newQueue(c.postBatch, cfg.FlushAt, cfg.MaxQueueSize, cfg.MaxRetries, cfg.MaxBatchSize, cfg.FlushInterval, c.logf, c.batchDropped)
	if c.secret != "" {
		go c.poll()
		if !cfg.DisableFlagStream {
			go c.streamLoop()
		}
	} else {
		close(c.ready)
	}
	return c
}

func orStr(v, d string) string {
	if v == "" {
		return d
	}
	return v
}

func orInt(v, d int) int {
	if v <= 0 {
		return d
	}
	return v
}

func orDur(v, d time.Duration) time.Duration {
	if v <= 0 {
		return d
	}
	return v
}

// DroppedEvents counts events discarded because the queue was full or the server kept failing.
func (c *Client) DroppedEvents() int64 {
	if c.queue == nil {
		return 0
	}
	return c.queue.dropped.Load()
}

// Capture queues an event; it never blocks on the network. It needs in.DistinctID (or Personless).
func (c *Client) Capture(in CaptureInput) { c.CaptureContext(context.Background(), in) }

// Identify sets person properties for distinctID. An identical call (same id and properties) already
// sent by this client is skipped.
func (c *Client) Identify(distinctID string, set, setOnce map[string]any) {
	if !c.identity.first("identify", distinctID, set, setOnce) {
		return
	}
	c.Capture(CaptureInput{DistinctID: distinctID, Event: "$identify", Set: set, SetOnce: setOnce})
}

// Alias links alias to distinctID.
func (c *Client) Alias(distinctID, alias string) {
	c.Capture(CaptureInput{DistinctID: distinctID, Event: "$create_alias", Properties: map[string]any{"alias": alias, "distinct_id": distinctID}})
}

// GroupIdentify sets group properties. distinctID may be empty ("$<type>_<key>" is used).
func (c *Client) GroupIdentify(groupType, groupKey string, props map[string]any, distinctID string) {
	if !c.identity.first("group", groupType, groupKey, distinctID, props) {
		return
	}
	if distinctID == "" {
		distinctID = "$" + groupType + "_" + groupKey
	}
	if props == nil {
		props = map[string]any{}
	}
	c.Capture(CaptureInput{DistinctID: distinctID, Event: "$groupidentify",
		Properties: map[string]any{"$group_type": groupType, "$group_key": groupKey, "$group_set": props}})
}

// Flush sends everything queued, or returns ctx's error.
func (c *Client) Flush(ctx context.Context) error {
	if c.queue == nil {
		return nil
	}
	return c.queue.flush(ctx)
}

// Shutdown stops polling, flushes queued events and stops accepting new ones.
func (c *Client) Shutdown(ctx context.Context) error {
	c.pollOnce.Do(func() { close(c.stopPoll) })
	if p := c.cfg.FlagDefinitionCache; p != nil {
		func() {
			defer func() { _ = recover() }()
			p.Shutdown()
		}()
	}
	if c.queue == nil {
		return nil
	}
	return c.queue.shutdown(ctx)
}

// do POSTs (or GETs when payload is nil) and returns status (0 on network error), Retry-After and the body.
func (c *Client) do(ctx context.Context, method, url string, payload any, hdr map[string]string) (int, time.Duration, []byte) {
	return c.doTimeout(ctx, c.cfg.Timeout, method, url, payload, hdr)
}

func (c *Client) doTimeout(ctx context.Context, timeout time.Duration, method, url string, payload any, hdr map[string]string) (int, time.Duration, []byte) {
	var body io.Reader
	header := http.Header{}
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return 0, 0, nil
		}
		header.Set("Content-Type", "application/json")
		if len(raw) >= minGzipBytes {
			var z bytes.Buffer
			zw := gzip.NewWriter(&z)
			_, _ = zw.Write(raw)
			_ = zw.Close()
			raw = z.Bytes()
			sep := "?"
			if strings.Contains(url, "?") {
				sep = "&"
			}
			url += sep + "compression=gzip"
		}
		body = bytes.NewReader(raw)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return 0, 0, nil
	}
	for k, v := range header {
		req.Header[k] = v
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		c.logf("%s %s failed: %v", method, url, err)
		return 0, 0, nil
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	var ra time.Duration
	if s, err := strconv.ParseFloat(resp.Header.Get("Retry-After"), 64); err == nil && s > 0 {
		ra = min(time.Duration(s*float64(time.Second)), time.Minute)
	}
	return resp.StatusCode, ra, out
}

func (c *Client) postBatch(batch []wireEvent) (int, time.Duration) {
	status, ra, _ := c.do(context.Background(), http.MethodPost, c.host+"/v1/batch",
		map[string]any{"api_key": c.apiKey, "batch": batch, "sent_at": time.Now().UTC().Format(time.RFC3339Nano)}, nil)
	return status, ra
}

// ImportEvent is a historical event for ImportEvents.
type ImportEvent = CaptureInput

// ImportEvents sends historical events via POST /v1/s2s/batch (secret key, timestamps preserved) in
// chunks of 1000. It returns the number accepted and an error at the first rejected chunk.
func (c *Client) ImportEvents(ctx context.Context, events []ImportEvent) (int, error) {
	if c.sink != nil {
		for _, e := range events {
			c.sink.add(newWire(e))
		}
		return len(events), nil
	}
	if c.secret == "" {
		return 0, errors.New("lucida: ImportEvents requires SecretKey")
	}
	if c.cfg.Disabled {
		return 0, nil
	}
	sent := 0
	for i := 0; i < len(events); i += maxBatch {
		chunk := make([]wireEvent, 0, maxBatch)
		for _, e := range events[i:min(i+maxBatch, len(events))] {
			chunk = append(chunk, newWire(e))
		}
		status, _, _ := c.do(ctx, http.MethodPost, c.host+"/v1/s2s/batch",
			map[string]any{"batch": chunk, "sent_at": time.Now().UTC().Format(time.RFC3339Nano)},
			map[string]string{"Authorization": "Bearer " + c.secret})
		if status < 200 || status >= 300 {
			return sent, fmt.Errorf("lucida: import failed: status %d after %d events", status, sent)
		}
		sent += len(chunk)
	}
	return sent, nil
}
