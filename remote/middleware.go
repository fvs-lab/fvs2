package remote

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// rateLimiter is a per-account token bucket. A zero rate disables limiting.
type rateLimiter struct {
	ratePerSec float64
	burst      float64
	mu         sync.Mutex
	buckets    map[string]*bucket
}

type bucket struct {
	tokens float64
	last   time.Time
}

func newRateLimiter(ratePerSec float64, burst int) *rateLimiter {
	return &rateLimiter{
		ratePerSec: ratePerSec,
		burst:      float64(burst),
		buckets:    map[string]*bucket{},
	}
}

// allow consumes one token for account, returning false when the bucket is
// empty.
func (r *rateLimiter) allow(account string) bool {
	if r == nil || r.ratePerSec <= 0 {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	b := r.buckets[account]
	if b == nil {
		b = &bucket{tokens: r.burst, last: now}
		r.buckets[account] = b
	}
	b.tokens += now.Sub(b.last).Seconds() * r.ratePerSec
	if b.tokens > r.burst {
		b.tokens = r.burst
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// metrics holds server-wide counters exposed in Prometheus text format.
type metrics struct {
	requests      atomic.Int64
	requestErrors atomic.Int64
	blocksAdded   atomic.Int64
	bytesUploaded atomic.Int64
	bytesServed   atomic.Int64
	rateLimited   atomic.Int64
	quotaRejected atomic.Int64
	started       time.Time
}

func newMetrics() *metrics { return &metrics{started: time.Now()} }

func (m *metrics) write(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "# HELP fvs_requests_total Requests handled.\n")
	fmt.Fprintf(w, "# TYPE fvs_requests_total counter\n")
	fmt.Fprintf(w, "fvs_requests_total %d\n", m.requests.Load())
	fmt.Fprintf(w, "fvs_request_errors_total %d\n", m.requestErrors.Load())
	fmt.Fprintf(w, "fvs_blocks_added_total %d\n", m.blocksAdded.Load())
	fmt.Fprintf(w, "fvs_bytes_uploaded_total %d\n", m.bytesUploaded.Load())
	fmt.Fprintf(w, "fvs_bytes_served_total %d\n", m.bytesServed.Load())
	fmt.Fprintf(w, "fvs_rate_limited_total %d\n", m.rateLimited.Load())
	fmt.Fprintf(w, "fvs_quota_rejected_total %d\n", m.quotaRejected.Load())
	fmt.Fprintf(w, "fvs_uptime_seconds %d\n", int64(time.Since(m.started).Seconds()))
}

// auditLog appends one JSON line per mutating request.
type auditLog struct {
	mu sync.Mutex
	f  *os.File
}

func newAuditLog(path string) (*auditLog, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	return &auditLog{f: f}, nil
}

type auditEntry struct {
	Time    string `json:"time"`
	Account string `json:"account"`
	Method  string `json:"method"`
	Path    string `json:"path"`
	Status  int    `json:"status"`
}

func (a *auditLog) record(account, method, path string, status int) {
	if a == nil {
		return
	}
	entry := auditEntry{
		Time:    time.Now().UTC().Format(time.RFC3339),
		Account: account,
		Method:  method,
		Path:    path,
		Status:  status,
	}
	b, err := json.Marshal(entry)
	if err != nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	_, _ = a.f.Write(append(b, '\n'))
}

func (a *auditLog) Close() error {
	if a == nil {
		return nil
	}
	return a.f.Close()
}

// statusRecorder captures the response status so middleware can observe it.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(b)
}

func isMutating(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodDelete:
		return true
	default:
		return false
	}
}
