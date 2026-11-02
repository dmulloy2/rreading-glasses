package internal

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"
)

// RateLimiter is a token bucket for an upstream that meters queries, like
// Hardcover, where each top-level GraphQL field costs one token. It refills at
// a steady rate up to a fixed burst, and callers reserve tokens before they
// send, so callers are served in the order they asked and a large request
// can't be starved by small ones. It is safe for concurrent use.
type RateLimiter struct {
	mu       sync.Mutex
	capacity float64
	perSec   float64
	tokens   float64 // Negative when callers have reserved more than we have.
	last     time.Time
	now      func() time.Time
}

// NewRateLimiter returns a full limiter that allows perMinute queries a minute
// with bursts of up to burst. It returns nil, which never waits, when
// perMinute is zero or less.
func NewRateLimiter(perMinute, burst int) *RateLimiter {
	if perMinute <= 0 {
		return nil
	}
	return newRateLimiter(perMinute, burst, time.Now)
}

func newRateLimiter(perMinute, burst int, now func() time.Time) *RateLimiter {
	return &RateLimiter{
		capacity: float64(burst),
		perSec:   float64(perMinute) / 60,
		tokens:   float64(burst),
		last:     now(),
		now:      now,
	}
}

// refill adds the tokens earned since the last call. The caller holds mu.
func (l *RateLimiter) refill() {
	now := l.now()
	l.tokens = min(l.capacity, l.tokens+now.Sub(l.last).Seconds()*l.perSec)
	l.last = now
}

// reserve spends n tokens and returns how long the caller must wait before
// using them. The bucket goes into debt rather than refusing, so later
// callers queue up behind this one and the long-run rate holds however large
// n is.
func (l *RateLimiter) reserve(n int) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.refill()
	l.tokens -= float64(n)
	if l.tokens >= 0 {
		return 0
	}
	return time.Duration(-l.tokens / l.perSec * float64(time.Second))
}

// release returns n reserved tokens that were never used.
func (l *RateLimiter) release(n int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.refill()
	l.tokens = min(l.capacity, l.tokens+float64(n))
}

// Wait reserves n tokens and blocks until they are available. If ctx ends
// first the reservation is returned and ctx's error is returned.
func (l *RateLimiter) Wait(ctx context.Context, n int) error {
	if l == nil {
		return nil
	}

	wait := l.reserve(n)
	if wait <= 0 {
		return nil
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		l.release(n)
		return ctx.Err()
	}
}

// Cooldown empties the bucket after the upstream says we're over its limit,
// so every caller waits for a fresh refill instead of trying again at once.
func (l *RateLimiter) Cooldown() {
	if l == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.refill()
	l.tokens = min(l.tokens, 0)
}

type queryCostKey struct{}

// WithQueryCost records how many queries a request carries, for
// PacedTransport. A batched GraphQL request costs one per top-level field.
func WithQueryCost(ctx context.Context, n int) context.Context {
	return context.WithValue(ctx, queryCostKey{}, n)
}

func queryCost(ctx context.Context) int {
	if n, ok := ctx.Value(queryCostKey{}).(int); ok && n > 0 {
		return n
	}
	return 1
}

// PacedTransport waits for rate-limit budget before every attempt it sends,
// including the retries RetryTransport makes above it, and empties the bucket
// when the upstream answers 429. Each attempt gets its own timeout once it is
// on its way, so time spent waiting for budget doesn't count against it.
type PacedTransport struct {
	http.RoundTripper

	Limiter *RateLimiter
	// Timeout bounds a single attempt once it is sent. Defaults to 60s.
	Timeout time.Duration
}

// RoundTrip implements http.RoundTripper.
func (t PacedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if err := t.Limiter.Wait(r.Context(), queryCost(r.Context())); err != nil {
		return nil, err
	}

	timeout := t.Timeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)

	resp, err := t.RoundTripper.RoundTrip(r.WithContext(ctx))
	if err != nil {
		cancel()
		return nil, err
	}

	if resp.StatusCode == http.StatusTooManyRequests {
		t.Limiter.Cooldown()
	}

	// The attempt's context has to outlive RoundTrip until the body is read.
	resp.Body = cancelOnClose{ReadCloser: resp.Body, cancel: cancel}
	return resp, nil
}

type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c cancelOnClose) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}
