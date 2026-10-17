package backoff

import (
	"testing"
	"time"
)

func TestCeiling(t *testing.T) {
	t.Parallel()
	p := Policy{Base: time.Second, Max: time.Hour}
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: -3, want: time.Second},
		{attempt: 0, want: time.Second},
		{attempt: 1, want: time.Second},
		{attempt: 2, want: 2 * time.Second},
		{attempt: 3, want: 4 * time.Second},
		{attempt: 10, want: 512 * time.Second},
		{attempt: 12, want: 2048 * time.Second},
		{attempt: 13, want: time.Hour},
		{attempt: 1000, want: time.Hour},
		{attempt: int(^uint(0) >> 1), want: time.Hour},
	}
	for _, tc := range tests {
		if got := p.Ceiling(tc.attempt); got != tc.want {
			t.Errorf("Ceiling(%d) = %v, want %v", tc.attempt, got, tc.want)
		}
	}
}

func TestCeilingDegenerate(t *testing.T) {
	t.Parallel()
	tests := []Policy{
		{},
		{Base: time.Second},
		{Max: time.Second},
		{Base: -time.Second, Max: time.Second},
	}
	for _, p := range tests {
		if got := p.Ceiling(3); got != 0 {
			t.Errorf("%+v.Ceiling(3) = %v, want 0", p, got)
		}
		if got := p.Delay(3); got != 0 {
			t.Errorf("%+v.Delay(3) = %v, want 0", p, got)
		}
	}
}

func TestCeilingBaseAboveMax(t *testing.T) {
	t.Parallel()
	p := Policy{Base: time.Minute, Max: time.Second}
	for _, attempt := range []int{1, 2, 5} {
		if got := p.Ceiling(attempt); got != time.Second {
			t.Errorf("Ceiling(%d) = %v, want 1s", attempt, got)
		}
	}
}

func TestDelayUsesFullJitter(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		rnd  float64
		want time.Duration
	}{
		{name: "lower bound", rnd: 0, want: 0},
		{name: "midpoint", rnd: 0.5, want: 4 * time.Second},
		{name: "near upper bound", rnd: 0.999, want: 7992 * time.Millisecond},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := Policy{Base: time.Second, Max: time.Minute, Rand: func() float64 { return tc.rnd }}
			if got := p.Delay(4); got != tc.want {
				t.Fatalf("Delay(4) = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDelayStaysWithinCeiling(t *testing.T) {
	t.Parallel()
	p := Policy{Base: 100 * time.Millisecond, Max: 30 * time.Second}
	for attempt := 1; attempt <= 20; attempt++ {
		c := p.Ceiling(attempt)
		var sum time.Duration
		const n = 2000
		for range n {
			d := p.Delay(attempt)
			if d < 0 || d >= c {
				t.Fatalf("Delay(%d) = %v outside [0, %v)", attempt, d, c)
			}
			sum += d
		}
		mean := sum / n
		if mean < c*35/100 || mean > c*65/100 {
			t.Fatalf("attempt %d: mean %v not near %v", attempt, mean, c/2)
		}
	}
}

func BenchmarkDelay(b *testing.B) {
	p := Policy{Base: time.Second, Max: time.Hour}
	for b.Loop() {
		_ = p.Delay(8)
	}
}
