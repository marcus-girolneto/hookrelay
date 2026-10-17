// Package backoff computes retry delays using capped exponential backoff with
// full jitter, as described in
// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/.
package backoff

import (
	"math/rand/v2"
	"time"
)

// Policy describes a capped exponential backoff schedule. The zero value is
// not useful; construct one with explicit Base and Max.
type Policy struct {
	// Base is the ceiling for the first retry.
	Base time.Duration
	// Max caps the ceiling for every retry.
	Max time.Duration
	// Rand returns a float in [0, 1). It defaults to math/rand/v2.Float64
	// and exists so tests can make jitter deterministic.
	Rand func() float64
}

// Ceiling returns min(Max, Base*2^(attempt-1)) for attempt >= 1. Attempt
// values below 1 are treated as 1.
func (p Policy) Ceiling(attempt int) time.Duration {
	if p.Base <= 0 || p.Max <= 0 {
		return 0
	}
	attempt = max(attempt, 1)
	d := p.Base
	for range attempt - 1 {
		// Doubling past Max (or overflowing) stops the loop early, so large
		// attempt numbers cost at most ~63 iterations.
		if d >= p.Max || d > p.Max/2 {
			return p.Max
		}
		d *= 2
	}
	return min(d, p.Max)
}

// Delay returns a uniformly random duration in [0, Ceiling(attempt)).
//
// Full jitter spreads retries from many failing deliveries across the whole
// window instead of synchronising them into waves, which matters when one
// receiver outage puts thousands of deliveries on the same schedule.
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
