package goclient

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetchWithBackoff(t *testing.T) {
	t.Run("SuccessOnFirstAttempt", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("success"))
		}))
		defer server.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		req, err := http.NewRequest("GET", server.URL, nil)
		require.NoError(t, err)

		client := &http.Client{}
		opts := BackoffOptions{
			InitialDelay: 50 * time.Millisecond,
			MaxDelay:     500 * time.Millisecond,
			Multiplier:   2.0,
			MaxRetries:   3,
		}

		start := time.Now()
		resp, err := FetchWithBackoff(ctx, client, req, opts)
		elapsed := time.Since(start)

		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		// Should not have taken long since it succeeded on first attempt
		assert.Less(t, elapsed, 100*time.Millisecond)

		resp.Body.Close()
	})

	t.Run("RetryOnServerError", func(t *testing.T) {
		attemptCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attemptCount++
			if attemptCount < 3 {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("server error"))
			} else {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("success"))
			}
		}))
		defer server.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		req, err := http.NewRequest("GET", server.URL, nil)
		require.NoError(t, err)

		client := &http.Client{}
		opts := BackoffOptions{
			InitialDelay: 10 * time.Millisecond,
			MaxDelay:     100 * time.Millisecond,
			Multiplier:   2.0,
			MaxRetries:   5,
		}

		start := time.Now()
		resp, err := FetchWithBackoff(ctx, client, req, opts)
		elapsed := time.Since(start)

		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, 3, attemptCount) // Should have made 3 attempts

		// Should have taken some time due to retries
		assert.Greater(t, elapsed, 20*time.Millisecond)

		resp.Body.Close()
	})

	t.Run("RetryOn429TooManyRequests", func(t *testing.T) {
		attemptCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attemptCount++
			if attemptCount < 2 {
				w.WriteHeader(http.StatusTooManyRequests)
				w.Write([]byte("too many requests"))
			} else {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("success"))
			}
		}))
		defer server.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		req, err := http.NewRequest("GET", server.URL, nil)
		require.NoError(t, err)

		client := &http.Client{}
		opts := BackoffOptions{
			InitialDelay: 10 * time.Millisecond,
			MaxDelay:     100 * time.Millisecond,
			Multiplier:   2.0,
			MaxRetries:   3,
		}

		resp, err := FetchWithBackoff(ctx, client, req, opts)

		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, 2, attemptCount) // Should have retried once

		resp.Body.Close()
	})

	t.Run("NoRetryOnClientError", func(t *testing.T) {
		attemptCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attemptCount++
			w.WriteHeader(http.StatusBadRequest) // 4xx error
			w.Write([]byte("bad request"))
		}))
		defer server.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		req, err := http.NewRequest("GET", server.URL, nil)
		require.NoError(t, err)

		client := &http.Client{}
		opts := BackoffOptions{
			InitialDelay: 10 * time.Millisecond,
			MaxDelay:     100 * time.Millisecond,
			Multiplier:   2.0,
			MaxRetries:   3,
		}

		start := time.Now()
		resp, err := FetchWithBackoff(ctx, client, req, opts)
		elapsed := time.Since(start)

		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.Equal(t, 1, attemptCount) // Should not have retried

		// Should have returned quickly
		assert.Less(t, elapsed, 50*time.Millisecond)

		resp.Body.Close()
	})

	t.Run("ExceedMaxRetries", func(t *testing.T) {
		attemptCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attemptCount++
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("server error"))
		}))
		defer server.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		req, err := http.NewRequest("GET", server.URL, nil)
		require.NoError(t, err)

		client := &http.Client{}
		opts := BackoffOptions{
			InitialDelay: 10 * time.Millisecond,
			MaxDelay:     50 * time.Millisecond,
			Multiplier:   2.0,
			MaxRetries:   2, // Only allow 2 retries
		}

		resp, err := FetchWithBackoff(ctx, client, req, opts)

		assert.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, 2, attemptCount) // Should have made max attempts
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Always return server error to force retries
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("server error"))
		}))
		defer server.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		req, err := http.NewRequest("GET", server.URL, nil)
		require.NoError(t, err)

		client := &http.Client{}
		opts := BackoffOptions{
			InitialDelay: 20 * time.Millisecond,
			MaxDelay:     100 * time.Millisecond,
			Multiplier:   2.0,
			MaxRetries:   5,
		}

		resp, err := FetchWithBackoff(ctx, client, req, opts)

		assert.Error(t, err)
		assert.Nil(t, resp)

		// Should be a context cancellation or FetchBackoffAbortError
		if !errors.Is(err, context.DeadlineExceeded) {
			_, isFetchBackoffError := err.(*FetchBackoffAbortError)
			assert.True(t, isFetchBackoffError, "Expected context cancellation or FetchBackoffAbortError")
		}
	})

	t.Run("BackoffProgression", func(t *testing.T) {
		attemptTimes := []time.Time{}
		attemptCount := 0

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attemptTimes = append(attemptTimes, time.Now())
			attemptCount++
			if attemptCount < 4 {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("server error"))
			} else {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("success"))
			}
		}))
		defer server.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		req, err := http.NewRequest("GET", server.URL, nil)
		require.NoError(t, err)

		client := &http.Client{}
		opts := BackoffOptions{
			InitialDelay: 50 * time.Millisecond,
			MaxDelay:     500 * time.Millisecond,
			Multiplier:   2.0,
			MaxRetries:   5,
		}

		resp, err := FetchWithBackoff(ctx, client, req, opts)

		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, 4, len(attemptTimes))

		// Check that delays are increasing (approximately)
		if len(attemptTimes) >= 3 {
			delay1 := attemptTimes[1].Sub(attemptTimes[0])
			delay2 := attemptTimes[2].Sub(attemptTimes[1])

			// Second delay should be roughly double the first
			// (allowing for some variance due to timing)
			assert.Greater(t, delay2, delay1)
			assert.Less(t, delay2, delay1*3) // Not more than 3x
		}

		resp.Body.Close()
	})

	t.Run("OnFailedAttemptCallback", func(t *testing.T) {
		failedAttempts := 0
		attemptCount := 0

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attemptCount++
			if attemptCount < 3 {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("server error"))
			} else {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("success"))
			}
		}))
		defer server.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		req, err := http.NewRequest("GET", server.URL, nil)
		require.NoError(t, err)

		client := &http.Client{}
		opts := BackoffOptions{
			InitialDelay: 10 * time.Millisecond,
			MaxDelay:     100 * time.Millisecond,
			Multiplier:   2.0,
			MaxRetries:   5,
			OnFailedAttempt: func() {
				failedAttempts++
			},
		}

		resp, err := FetchWithBackoff(ctx, client, req, opts)

		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, 2, failedAttempts) // Should have called callback for 2 failed attempts

		resp.Body.Close()
	})

	t.Run("DefaultValues", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("success"))
		}))
		defer server.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		req, err := http.NewRequest("GET", server.URL, nil)
		require.NoError(t, err)

		client := &http.Client{}

		// Use empty options to test defaults
		opts := BackoffOptions{}

		resp, err := FetchWithBackoff(ctx, client, req, opts)

		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		resp.Body.Close()
	})
}
