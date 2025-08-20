package goclient

import (
	"context"
	"errors"
	"net/http"
	"time"
)

type BackoffOptions struct {
	InitialDelay    time.Duration
	MaxDelay        time.Duration
	Multiplier      float64
	OnFailedAttempt func()
	OnFailedRetries func(error)
	MaxRetries      int
}

var BackoffDefaults = BackoffOptions{
	InitialDelay: 100 * time.Millisecond,
	MaxDelay:     10 * time.Second,
	Multiplier:   1.3,
	MaxRetries:   5, // Reasonable default, can be overridden
}

// FetchWithBackoff wraps an http.Client Do with exponential backoff on retriable errors.
func FetchWithBackoff(ctx context.Context, client *http.Client, req *http.Request, opts BackoffOptions) (*http.Response, error) {
	delay := opts.InitialDelay
	if delay <= 0 {
		delay = BackoffDefaults.InitialDelay
	}
	max := opts.MaxDelay
	if max <= 0 {
		max = BackoffDefaults.MaxDelay
	}
	mult := opts.Multiplier
	if mult <= 1.0 {
		mult = BackoffDefaults.Multiplier
	}
	maxRetries := opts.MaxRetries
	if maxRetries <= 0 {
		maxRetries = BackoffDefaults.MaxRetries
	}

	var lastErr error
	attempt := 0

	for attempt < maxRetries {
		// Respect context cancellations
		if ctx.Err() != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				return nil, &FetchBackoffAbortError{}
			}
			return nil, ctx.Err()
		}

		resp, err := client.Do(req.Clone(ctx))
		if err == nil {
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return resp, nil
			}
			// Non-2xx: decide retriable
			if resp.StatusCode == http.StatusTooManyRequests { // 429
				// retry
			} else if resp.StatusCode >= 400 && resp.StatusCode < 500 {
				// non-retriable client error
				return resp, nil
			} else {
				// server errors retriable
			}
			lastErr = &FetchError{
				Status: resp.StatusCode,
				URL:    req.URL.String(),
			}
		} else {
			lastErr = err
		}

		if opts.OnFailedAttempt != nil {
			opts.OnFailedAttempt()
		}

		attempt++
		if attempt >= maxRetries {
			break
		}

		select {
		case <-time.After(delay):
			// increase delay
			next := time.Duration(float64(delay) * mult)
			if next > max {
				next = max
			}
			delay = next
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.Canceled) {
				return nil, &FetchBackoffAbortError{}
			}
			return nil, ctx.Err()
		}
	}

	if opts.OnFailedRetries != nil {
		opts.OnFailedRetries(lastErr)
	}

	return nil, lastErr
}
