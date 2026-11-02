package internal

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blampe/rreading-glasses/gr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRateLimiterReserve(t *testing.T) {
	now := time.Unix(1000, 0)
	l := newRateLimiter(60, 10, func() time.Time { return now }) // 1/s, burst 10.

	assert.Equal(t, time.Duration(0), l.reserve(10), "a new limiter is full")
	assert.Equal(t, time.Second, l.reserve(1), "the next token is a second away")
	assert.Equal(t, 2*time.Second, l.reserve(1), "and callers queue up behind each other")

	now = now.Add(2 * time.Second)
	assert.Equal(t, time.Second, l.reserve(1), "time pays down the debt")

	now = now.Add(time.Hour)
	assert.Equal(t, time.Duration(0), l.reserve(10), "refill is capped at the burst")
	assert.Equal(t, 10*time.Second, l.reserve(10))
	assert.Equal(t, 25*time.Second, l.reserve(15), "a request larger than the burst just waits longer")
}

func TestRateLimiterReleaseAndCooldown(t *testing.T) {
	now := time.Unix(1000, 0)
	l := newRateLimiter(60, 10, func() time.Time { return now })

	l.reserve(10)
	assert.Equal(t, 5*time.Second, l.reserve(5))
	l.release(5)
	assert.Equal(t, time.Second, l.reserve(1), "a released reservation is no longer waited for")

	now = now.Add(time.Hour)
	l.Cooldown()
	assert.Equal(t, time.Second, l.reserve(1), "a cooldown empties a full bucket")

	var none *RateLimiter
	require.NoError(t, none.Wait(context.Background(), 100), "a nil limiter never waits")
}

func okResponse() *http.Response {
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"data": {}, "errors": []}`))}
}

func TestBatchingPaced(t *testing.T) {
	var mu sync.Mutex
	var sent []time.Time

	client := &http.Client{Transport: PacedTransport{
		Limiter: NewRateLimiter(600, 2),
		RoundTripper: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			mu.Lock()
			sent = append(sent, time.Now())
			mu.Unlock()
			return okResponse(), nil
		}),
	}}

	// Batches of 2 queries, refilling at 10 a second: the first batch goes
	// out at once, and each one after waits for another 2 tokens (~200ms).
	gql, err := NewBatchedGraphQLClient("https://foo.com", client, 10*time.Millisecond, 2, nil)
	require.NoError(t, err)

	start := time.Now()

	wg := sync.WaitGroup{}
	errs := make([]error, 6)
	for i := range errs {
		wg.Go(func() {
			_, errs[i] = gr.GetBook(t.Context(), gql, int64(i+1))
		})
	}
	wg.Wait()

	for _, err := range errs {
		assert.NoError(t, err)
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, sent, 3)
	assert.Less(t, sent[0].Sub(start), 150*time.Millisecond, "the first batch spends the burst")
	assert.GreaterOrEqual(t, sent[1].Sub(sent[0]), 150*time.Millisecond, "the second waits for tokens")
	assert.GreaterOrEqual(t, sent[2].Sub(sent[1]), 150*time.Millisecond, "and so does the third")
}

func TestRetriesArePaced(t *testing.T) {
	var mu sync.Mutex
	var sent []time.Time

	// The first attempt is refused with a 429; the retry must wait for the
	// bucket to refill rather than going straight back out.
	transport := RetryTransport{
		Attempts: 2,
		Backoff:  time.Millisecond,
		RoundTripper: PacedTransport{
			Limiter: NewRateLimiter(600, 2), // 10/s, burst 2.
			RoundTripper: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				mu.Lock()
				defer mu.Unlock()
				sent = append(sent, time.Now())
				if len(sent) == 1 {
					return &http.Response{StatusCode: http.StatusTooManyRequests, Body: io.NopCloser(strings.NewReader(""))}, nil
				}
				return okResponse(), nil
			}),
		},
	}

	req, err := http.NewRequestWithContext(WithQueryCost(t.Context(), 2), http.MethodGet, "https://foo.com", nil)
	require.NoError(t, err)

	resp, err := transport.RoundTrip(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, sent, 2)
	assert.GreaterOrEqual(t, sent[1].Sub(sent[0]), 150*time.Millisecond,
		"after a 429 the retry waits for 2 fresh tokens (~200ms), not just the 1ms backoff")
}

func TestAbandonedQueriesAreNotSent(t *testing.T) {
	calls := atomic.Int32{}

	limiter := NewRateLimiter(600, 1) // 10/s, burst 1.
	limiter.reserve(1)                // Spend the burst, so the next query waits ~100ms.

	client := &http.Client{Transport: PacedTransport{
		Limiter: limiter,
		RoundTripper: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			calls.Add(1)
			return okResponse(), nil
		}),
	}}

	gql, err := NewBatchedGraphQLClient("https://foo.com", client, 5*time.Millisecond, 5, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = gr.GetBook(ctx, gql, 1)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), 80*time.Millisecond, "the caller gets its answer when it gives up, not after the wait")

	time.Sleep(250 * time.Millisecond) // Long past when the budget would have allowed it.
	assert.Zero(t, calls.Load(), "a query nobody is waiting for is never sent")
}

func TestCanceledSubscribersAreDroppedFromABatch(t *testing.T) {
	var mu sync.Mutex
	var bodies []map[string]any

	client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		bodies = append(bodies, body)
		mu.Unlock()
		return okResponse(), nil
	})}

	gql, err := NewBatchedGraphQLClient("https://foo.com", client, 50*time.Millisecond, 5, nil)
	require.NoError(t, err)

	gone, cancel := context.WithCancel(t.Context())
	cancel()

	wg := sync.WaitGroup{}
	var goneErr, liveErr error
	wg.Go(func() { _, goneErr = gr.GetBook(gone, gql, 1) })
	wg.Go(func() { _, liveErr = gr.GetBook(t.Context(), gql, 2) })
	wg.Wait()

	assert.ErrorIs(t, goneErr, context.Canceled)
	assert.NoError(t, liveErr)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, bodies, 1)
	vars, _ := bodies[0]["variables"].(map[string]any)
	assert.Len(t, vars, 1, "only the query still wanted is sent")
}
