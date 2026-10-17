// Package backoff computes capped exponential backoff with full jitter.
package backoff

import (
	"math/rand/v2"
	"time"
)

// Policy is a capped exponential backoff schedule.
type Policy struct {
	Base time.Duration
	Max  time.Duration
	Rand func() float64 // nil means math/rand/v2.Float64
}

// Ceiling returns min(Max, Base*2^(attempt-1)), treating attempt < 1 as 1.
func (p Policy) Ceiling(attempt int) time.Duration {
	if p.Base <= 0 || p.Max <= 0 {
		return 0
	}
	attempt = max(attempt, 1)
	d := p.Base
	for range attempt - 1 {
		if d >= p.Max || d > p.Max/2 {
			return p.Max
		}
		d *= 2
	}
	return min(d, p.Max)
}

// Delay returns a uniformly random duration in [0, Ceiling(attempt)).
func (p Policy) Delay(attempt int) time.Duration {
	c := p.Ceiling(attempt)
	if c <= 0 {
		return 0
	}
	r := rand.Float64
	if p.Rand != nil {
		r = p.Rand
	}
	return time.Duration(r() * float64(c))
}
