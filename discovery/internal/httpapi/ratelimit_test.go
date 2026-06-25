package httpapi

import (
	"strconv"
	"testing"

	"github.com/GhentiLabs/Trove/discovery/internal/config"
)

func TestLimiterStoreCapsEntries(t *testing.T) {
	s := newLimiterStore(config.RateLimit{RPS: 1e6, Burst: 1e6}, nil)
	total := maxLimiterEntries + 1000
	for i := range total {
		s.allow(strconv.Itoa(i))
	}
	s.mu.Lock()
	n := len(s.limiters)
	s.mu.Unlock()
	if n > maxLimiterEntries {
		t.Fatalf("limiter map grew to %d entries, want <= %d", n, maxLimiterEntries)
	}
}
