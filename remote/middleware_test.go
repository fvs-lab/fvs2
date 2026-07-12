package remote

import (
	"fmt"
	"testing"
	"time"
)

func TestRateLimiterPrunesIdleBuckets(t *testing.T) {
	r := newRateLimiter(1000, 1000)
	base := time.Now()
	// A burst of distinct accounts creates a bucket each.
	for i := 0; i < 500; i++ {
		r.allow(fmt.Sprintf("acct-%d", i))
	}
	if len(r.buckets) != 500 {
		t.Fatalf("want 500 buckets, got %d", len(r.buckets))
	}
	// Force the sweep clock forward and let those buckets go idle, then a new
	// request triggers the prune.
	r.mu.Lock()
	for _, b := range r.buckets {
		b.last = base.Add(-20 * time.Minute)
	}
	r.sweep = base.Add(-2 * time.Minute)
	r.mu.Unlock()
	r.allow("fresh")
	if len(r.buckets) != 1 {
		t.Fatalf("idle buckets not pruned: %d remain", len(r.buckets))
	}
}
