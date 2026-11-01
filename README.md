# lucida (Go)

Server SDK for Lucida. Standard library only, context-aware, safe for concurrent use. Module
`github.com/lucidaanalytics/lucida-go` (separate from the gateway's `go/go.mod`). Every call names its distinct id.

```go
c := lucida.New("lc_pk_...", lucida.Config{Host: "https://app.lucida.dev", SecretKey: "lc_sk_..."})
defer c.Shutdown(ctx) // flushes

c.Capture(lucida.CaptureInput{DistinctID: "u1", Event: "purchase", Properties: map[string]any{"total": 12}})
c.Identify("u1", map[string]any{"plan": "pro"}, nil)
c.Alias("u1", "anon-9")
c.GroupIdentify("company", "acme", map[string]any{"seats": 5}, "")

on, ok := c.IsFeatureEnabled(ctx, "new-checkout", "u1", lucida.FlagOptions{PersonProperties: map[string]any{"plan": "pro"}})
```

## Behaviour

- `Capture` never blocks on the network. A background goroutine flushes at `FlushAt` (20) events or `FlushInterval` (5 s).
- Bodies over 1 KB are gzipped and sent with `?compression=gzip`.
- Retry (network error, 408, 429, 5xx) with jittered exponential backoff honouring `Retry-After`; after `MaxRetries` (5)
  the batch is dropped. The queue is bounded (`MaxQueueSize`, 1000): the oldest events are dropped. `DroppedEvents()` counts both.
- `Flush(ctx)`, `Shutdown(ctx)` (flush, then stop; retries are abandoned if ctx ends first), `Config.Disabled`, `Config.Debug` / `Logf`.
- `ImportEvents(ctx, events)`: `POST /v1/s2s/batch` with the secret key, timestamps preserved, chunks of 1000.

## Request context and middleware

```go
ctx := lucida.WithRequestContext(r.Context(), lucida.RequestContext{DistinctID: "u1", SessionID: "s1", Properties: map[string]any{"svc": "api"}})
client.CaptureContext(ctx, lucida.CaptureInput{Event: "purchase"}) // id, $session_id, properties from ctx; explicit fields win
```

`WithRequestContext` returns a child context merged over the parent's (`Fresh: true` starts empty); the parent is never
modified. `ReadTracingHeaders(h)` reads the browser SDK's `X-Lucida-Distinct-Id` / `X-Lucida-Session-Id` (control chars
stripped, trimmed, 1000 chars; client-controlled, never use for auth).

`NewRequestContextMiddleware(client, lucida.RequestContextOptions{TrustProxy: true})` wraps an `http.Handler`: it stamps
`$current_url`, `$request_method`, `$request_path`, `$user_agent`, `$ip` (first `X-Forwarded-For` entry only with
`TrustProxy`) and the tracing ids; options `RequestFilter`, `ExtraProperties`, `NoTracingHeaders`.

## Delivery options

`Config.BeforeSend` (return nil to drop, edit in place otherwise; a panic keeps the original), `Config.OnError(err, events)`
(a `*DeliveryError` when a batch is given up on; also when an event has no distinct id), `MaxBatchSize` (per request,
separate from `FlushAt`), `DisableGeoip` / `CaptureInput.DisableGeoip` (sends `$geoip_disable: true`), `FlagTimeout`,
`FlagDedupCacheSize`. Personless events: `CaptureInput{Event: "e", Personless: true}` sends
`$process_person_profile: false` and a random UUID distinct id when none is given. `GetRemoteConfigPayload(ctx, key)`
(needs `SecretKey`) reads a remote-config flag's payload, decrypted when stored encrypted.

## Flags

With `SecretKey` the client polls `GET /v1/flags/definitions` (ETag / `If-None-Match`, every `FlagPollInterval` = 30 s;
`<-c.Ready()` fires after the first attempt; `ReloadFeatureFlags(ctx)` forces one) and evaluates locally
(`flageval_*.go`, a port of the gateway evaluator pinned by `testdata/flags`). Flags needing the server (cohort filters)
or unknown locally fall back to `POST /v1/flags` unless `FlagOptions.OnlyEvaluateLocally` (then `ok == false`).
Without a secret key every read is remote.

Sticky bucketing (LC-EXT-29): set `Config.StickyStore` (e.g. `lucida.NewMemoryStickyStore()`, or your own
`StickyStore` with `Get(bucketingKey)` / `Set(bucketingKey, flagKey, value)`) and units keep the value they got
from a `sticky` flag when its rollout or variant weights change (`EvaluateAllSticky` for direct use).

Helpers: `MigrateRead` / `MigrateWrite` run a migration flag's stage (`MigrationStageOf(value)`: off, dualwrite,
shadow, live, rampdown, complete) over `MigrationOps[R, W]` and report consistency, errors and latency to `OnReport`.
`AIConfigFrom(result, fallback, vars)` returns the model / provider / messages / parameters an AI Config flag
serves, with `{{var}}` placeholders filled, or `fallback` (`Enabled == false`) when nothing is served.

`GetFeatureFlag` emits `$feature_flag_called` and `$exposure` once per `(distinctID, flag, value)` (bounded LRU, 10 000;
`NoExposure` disables it). `GetFeatureFlagPayload` and `GetAllFlags` never emit.

## Tests

```sh
cd sdks/go && go vet ./... && go test -race ./...
```

`conformance_test.go` runs `testdata/flags/vectors*.json` (supplying only the context fields in `needs`) and the
`testdata/events/server_sdk.json` wire fixtures against an `httptest` server.

## Test mode

`lucida flags fixture --out flags.json`, then `c, err := lucida.NewTestClient("flags.json")` (or `NewTestClientWithFlags([]lucida.Flag{...})`):
no network and no goroutines, flags evaluate locally (unknown or server-only flags are not found), and `c.Captured()` returns every
event, including `$feature_flag_called` / `$exposure`, for assertions.

---

This repository is a read-only mirror of the Lucida monorepo (synced by a script, never edited by hand); releases and provenance attestations are built here by GitHub Actions. Issues are welcome, changes land upstream.
