package lucida

import (
	"net/http"
	"strings"
)

// RequestContextOptions configures NewRequestContextMiddleware.
type RequestContextOptions struct {
	// RequestFilter returns false to skip the context for a request (health checks...).
	RequestFilter func(r *http.Request) bool
	// ExtraProperties are stamped on every request's context.
	ExtraProperties map[string]any
	// NoTracingHeaders stops reading X-Lucida-Distinct-Id / X-Lucida-Session-Id.
	NoTracingHeaders bool
	// TrustProxy takes $ip from the first X-Forwarded-For entry; otherwise the socket peer.
	TrustProxy bool
}

// NewRequestContextMiddleware returns net/http middleware that puts a fresh RequestContext in the request's
// context: distinct and session id from the browser SDK's tracing headers (client-controlled; never use them for
// auth) and the properties $current_url, $request_method, $request_path, $user_agent and $ip. Handlers call
// client.CaptureContext(r.Context(), ...); to attach the logged-in user, wrap r.Context() with
// WithRequestContext(ctx, RequestContext{DistinctID: user.ID}) (a child context; the parent is untouched).
// The client is accepted so call sites stay stable if options later move onto it.
func NewRequestContextMiddleware(_ *Client, opts RequestContextOptions) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if opts.RequestFilter != nil && !opts.RequestFilter(r) {
				next.ServeHTTP(w, r)
				return
			}
			rc := RequestContext{Fresh: true, Properties: requestProps(r, opts)}
			if !opts.NoTracingHeaders {
				rc.DistinctID, rc.SessionID = ReadTracingHeaders(r.Header)
			}
			next.ServeHTTP(w, r.WithContext(WithRequestContext(r.Context(), rc)))
		})
	}
}

func requestProps(r *http.Request, opts RequestContextOptions) map[string]any {
	props := map[string]any{}
	set := func(k, v string) {
		if v != "" {
			props[k] = v
		}
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if r.Host != "" {
		set("$current_url", scheme+"://"+r.Host+r.URL.RequestURI())
	}
	set("$request_method", r.Method)
	set("$request_path", r.URL.Path)
	set("$user_agent", CleanHeader(r.UserAgent()))
	ip := r.RemoteAddr
	if i := strings.LastIndex(ip, ":"); i > 0 {
		ip = ip[:i]
	}
	ip = strings.Trim(ip, "[]")
	if opts.TrustProxy {
		if first, _, _ := strings.Cut(r.Header.Get("X-Forwarded-For"), ","); CleanHeader(first) != "" {
			ip = CleanHeader(first)
		}
	}
	set("$ip", ip)
	for k, v := range opts.ExtraProperties {
		props[k] = v
	}
	return props
}
