package httpapi

import (
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/GhentiLabs/Trove/discovery/internal/config"
)

// limiterStore holds one token-bucket limiter per source key (IP or node),
// reaping idle limiters so memory stays bounded under churn.
type limiterStore struct {
	mu       sync.Mutex
	limiters map[string]*keyedLimiter
	rps      rate.Limit
	burst    int
	clock    func() time.Time
}

type keyedLimiter struct {
	lim      *rate.Limiter
	lastSeen time.Time
}

func newLimiterStore(rl config.RateLimit, clock func() time.Time) *limiterStore {
	if clock == nil {
		clock = time.Now
	}
	return &limiterStore{
		limiters: make(map[string]*keyedLimiter),
		rps:      rate.Limit(rl.RPS),
		burst:    rl.Burst,
		clock:    clock,
	}
}

// allow reports whether one event is permitted for key right now.
func (s *limiterStore) allow(key string) bool {
	s.mu.Lock()
	kl, ok := s.limiters[key]
	if !ok {
		kl = &keyedLimiter{lim: rate.NewLimiter(s.rps, s.burst)}
		s.limiters[key] = kl
	}
	kl.lastSeen = s.clock()
	lim := kl.lim
	s.mu.Unlock()
	return lim.Allow()
}

// sweep removes limiters idle for longer than maxIdle.
func (s *limiterStore) sweep(maxIdle time.Duration) {
	cutoff := s.clock().Add(-maxIdle)
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, kl := range s.limiters {
		if kl.lastSeen.Before(cutoff) {
			delete(s.limiters, k)
		}
	}
}
