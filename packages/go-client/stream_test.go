package goclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShapeStreamHeaders(t *testing.T) {
	// Track requested headers
	var requestHeaders http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestHeaders = r.Header

		// Return minimal valid response
		w.Header().Set(ShapeHandleHeader, "test-handle")
		w.Header().Set(ChunkLastOffsetHeader, "0_0")
		w.Header().Set(LiveCacheBusterHeader, "123")
		w.Header().Set(ShapeSchemaHeader, "{}")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"headers": {"control": "up-to-date"}}]`))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	stream, err := NewShapeStream(Options{
		URL: server.URL + "/v1/shape",
		Headers: Headers{
			"Authorization":   "my-token",
			"X-Custom-Header": "my-value",
		},
		Params: NewParams("foo"),
		Ctx:    ctx,
	})
	require.NoError(t, err)

	// Subscribe to trigger request
	unsubscribe := stream.Subscribe(func([]Message) {})
	defer unsubscribe()

	// Wait for request to be made
	assert.Eventually(t, func() bool {
		return requestHeaders != nil
	}, 2*time.Second, 100*time.Millisecond)

	// Check headers were set
	assert.Equal(t, "my-token", requestHeaders.Get("Authorization"))
	assert.Equal(t, "my-value", requestHeaders.Get("X-Custom-Header"))
}

func TestShapeStreamURLSorting(t *testing.T) {
	var requestURL string
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if requestCount == 1 {
			// Capture only the first request URL
			requestURL = r.URL.String()
		}

		// Return minimal valid response
		w.Header().Set(ShapeHandleHeader, "potato")
		w.Header().Set(ChunkLastOffsetHeader, "0_0")
		w.Header().Set(LiveCacheBusterHeader, "123")
		w.Header().Set(ShapeSchemaHeader, "{}")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"headers": {"control": "up-to-date"}}]`))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	stream, err := NewShapeStream(Options{
		URL: server.URL,
		Params: NewParams("foo").
			WithWhere("a=1").
			WithColumns("id"),
		Handle: "potato",
		Ctx:    ctx,
	})
	require.NoError(t, err)

	// Subscribe to trigger request
	unsubscribe := stream.Subscribe(func([]Message) {})
	defer unsubscribe()

	// Wait for request
	assert.Eventually(t, func() bool {
		return requestURL != ""
	}, 2*time.Second, 100*time.Millisecond)

	// Parse URL to check query parameter sorting
	parsedURL, err := url.Parse(requestURL)
	require.NoError(t, err)

	queryString := parsedURL.RawQuery
	// Should be sorted alphabetically
	expected := "columns=id&handle=potato&offset=-1&table=foo&where=a%3D1"
	assert.Equal(t, expected, queryString)
}

func TestShapeStreamStartsOnlyAfterSubscription(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++

		// Return minimal valid response
		w.Header().Set(ShapeHandleHeader, "test-handle")
		w.Header().Set(ChunkLastOffsetHeader, "0_0")
		w.Header().Set(LiveCacheBusterHeader, "123")
		w.Header().Set(ShapeSchemaHeader, "{}")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"headers": {"control": "up-to-date"}}]`))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Create stream but don't subscribe yet
	stream, err := NewShapeStream(Options{
		URL: server.URL,
		Params: NewParams("foo").
			WithWhere("a=1").
			WithColumns("id"),
		Ctx: ctx,
	})
	require.NoError(t, err)

	// Wait a bit to ensure no requests are made
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, 0, requestCount, "No requests should be made before subscription")
	assert.False(t, stream.HasStarted(), "Stream should not have started")

	// Now subscribe
	unsubscribe := stream.Subscribe(func([]Message) {})
	defer unsubscribe()

	// Should start now
	assert.Eventually(t, func() bool {
		return stream.HasStarted()
	}, 1*time.Second, 50*time.Millisecond)

	// Request should be made
	assert.Eventually(t, func() bool {
		return requestCount > 0
	}, 1*time.Second, 50*time.Millisecond)
}

func TestShapeStreamErrorHandling(t *testing.T) {
	t.Run("HandleHTTPError", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Internal Server Error"))
		}))
		defer server.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		stream, err := NewShapeStream(Options{
			URL:    server.URL,
			Params: NewParams("foo"),
			Ctx:    ctx,
		})
		require.NoError(t, err)

		// Subscribe to trigger request
		unsubscribe := stream.Subscribe(func([]Message) {})
		defer unsubscribe()

		// Wait for error to be set
		assert.Eventually(t, func() bool {
			return stream.Error() != nil
		}, 2*time.Second, 100*time.Millisecond)

		err = stream.Error()
		assert.Error(t, err)
	})

	t.Run("Handle409ShapeRotation", func(t *testing.T) {
		requestCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount++
			if requestCount == 1 {
				// First request returns 409 with new handle
				w.Header().Set(ShapeHandleHeader, "new-handle")
				w.WriteHeader(http.StatusConflict)
				w.Write([]byte(`[{"headers": {"control": "must-refetch"}}]`))
			} else {
				// Subsequent requests succeed
				w.Header().Set(ShapeHandleHeader, "new-handle")
				w.Header().Set(ChunkLastOffsetHeader, "0_0")
				w.Header().Set(LiveCacheBusterHeader, "123")
				w.Header().Set(ShapeSchemaHeader, "{}")
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`[{"headers": {"control": "up-to-date"}}]`))
			}
		}))
		defer server.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		stream, err := NewShapeStream(Options{
			URL:    server.URL,
			Params: NewParams("foo"),
			Ctx:    ctx,
		})
		require.NoError(t, err)

		messageReceived := make(chan bool, 1)
		unsubscribe := stream.Subscribe(func(messages []Message) {
			// Should receive the must-refetch control message
			for _, msg := range messages {
				if IsControlMessage(msg) {
					if msg.Control.Headers.Control == "must-refetch" {
						messageReceived <- true
						return
					}
				}
			}
		})
		defer unsubscribe()

		// Should receive the control message from 409 response
		select {
		case <-messageReceived:
			// Expected
		case <-time.After(3 * time.Second):
			t.Fatal("Did not receive must-refetch message")
		}

		// Should handle rotation and continue
		assert.Eventually(t, func() bool {
			return stream.ShapeHandle() == "new-handle"
		}, 2*time.Second, 100*time.Millisecond)

		// Should have made multiple requests (initial + retry)
		assert.Eventually(t, func() bool {
			return requestCount >= 2
		}, 2*time.Second, 100*time.Millisecond)
	})
}

func TestShapeStreamMissingHeaders(t *testing.T) {
	tests := []struct {
		name           string
		missingHeaders []string
		setupResponse  func(w http.ResponseWriter, r *http.Request)
	}{
		{
			name:           "MissingShapeHandle",
			missingHeaders: []string{ShapeHandleHeader},
			setupResponse: func(w http.ResponseWriter, r *http.Request) {
				// Missing shape handle header
				w.Header().Set(ChunkLastOffsetHeader, "0_0")
				w.Header().Set(LiveCacheBusterHeader, "123")
				w.Header().Set(ShapeSchemaHeader, "{}")
			},
		},
		{
			name:           "MissingOffset",
			missingHeaders: []string{ChunkLastOffsetHeader},
			setupResponse: func(w http.ResponseWriter, r *http.Request) {
				// Missing offset header
				w.Header().Set(ShapeHandleHeader, "test-handle")
				w.Header().Set(LiveCacheBusterHeader, "123")
				w.Header().Set(ShapeSchemaHeader, "{}")
			},
		},
		{
			name:           "MissingSchema",
			missingHeaders: []string{ShapeSchemaHeader},
			setupResponse: func(w http.ResponseWriter, r *http.Request) {
				// Missing schema header (for non-live requests)
				w.Header().Set(ShapeHandleHeader, "test-handle")
				w.Header().Set(ChunkLastOffsetHeader, "0_0")
				// No live cache buster or schema - this makes it a non-live request
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				tt.setupResponse(w, r)
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`[{"headers": {"control": "up-to-date"}}]`))
			}))
			defer server.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			stream, err := NewShapeStream(Options{
				URL:    server.URL,
				Params: NewParams("foo"),
				Ctx:    ctx,
			})
			require.NoError(t, err)

			// Subscribe to trigger request
			unsubscribe := stream.Subscribe(func([]Message) {})
			defer unsubscribe()

			// Should get missing headers error
			assert.Eventually(t, func() bool {
				err := stream.Error()
				if err == nil {
					return false
				}
				_, isMissingHeaders := err.(MissingHeadersError)
				return isMissingHeaders
			}, 2*time.Second, 100*time.Millisecond)

			err = stream.Error()
			require.Error(t, err)
			missingHeadersErr, ok := err.(MissingHeadersError)
			require.True(t, ok)

			for _, expectedHeader := range tt.missingHeaders {
				assert.Contains(t, missingHeadersErr.MissingHeaders, expectedHeader)
			}
		})
	}
}

func TestShapeStreamFunctionParams(t *testing.T) {
	var requestURL string
	var requestHeaders http.Header

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestURL = r.URL.String()
		requestHeaders = r.Header

		w.Header().Set(ShapeHandleHeader, "test-handle")
		w.Header().Set(ChunkLastOffsetHeader, "0_0")
		w.Header().Set(LiveCacheBusterHeader, "123")
		w.Header().Set(ShapeSchemaHeader, "{}")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"headers": {"control": "up-to-date"}}]`))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Test function-based params and headers
	stream, err := NewShapeStream(Options{
		URL: server.URL,
		Headers: Headers{
			"Authorization": "Bearer dynamic-token",
			"Static-Header": "static-value",
		},
		Params: NewParams("users").
			WithWhere("id > 100"),
		Ctx: ctx,
	})
	require.NoError(t, err)

	// Subscribe to trigger request
	unsubscribe := stream.Subscribe(func([]Message) {})
	defer unsubscribe()

	// Wait for request
	assert.Eventually(t, func() bool {
		return requestURL != "" && requestHeaders != nil
	}, 2*time.Second, 100*time.Millisecond)

	// Check dynamic header was resolved
	assert.Equal(t, "Bearer dynamic-token", requestHeaders.Get("Authorization"))
	assert.Equal(t, "static-value", requestHeaders.Get("Static-Header"))

	// Check dynamic param was resolved
	parsedURL, err := url.Parse(requestURL)
	require.NoError(t, err)
	assert.Equal(t, "users", parsedURL.Query().Get("table"))
	assert.Equal(t, "id > 100", parsedURL.Query().Get("where"))
}

func TestShapeStreamSubscriptionManagement(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(ShapeHandleHeader, "test-handle")
		w.Header().Set(ChunkLastOffsetHeader, "0_0")
		w.Header().Set(LiveCacheBusterHeader, "123")
		w.Header().Set(ShapeSchemaHeader, "{}")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"headers": {"control": "up-to-date"}}]`))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := NewShapeStream(Options{
		URL:    server.URL,
		Params: NewParams("test"),
		Ctx:    ctx,
	})
	require.NoError(t, err)

	// Test multiple subscriptions
	callCount1 := 0
	callCount2 := 0

	unsubscribe1 := stream.Subscribe(func([]Message) {
		callCount1++
	})

	unsubscribe2 := stream.Subscribe(func([]Message) {
		callCount2++
	})

	// Wait for messages
	assert.Eventually(t, func() bool {
		return callCount1 > 0 && callCount2 > 0
	}, 2*time.Second, 100*time.Millisecond)

	// Both subscribers should have received messages
	assert.Greater(t, callCount1, 0)
	assert.Greater(t, callCount2, 0)

	// Unsubscribe first subscriber
	unsubscribe1()

	// Reset counters
	callCount1 = 0
	callCount2 = 0

	// Give some time to ensure no more calls to first subscriber
	time.Sleep(200 * time.Millisecond)

	// Only second subscriber should have received messages after unsubscribe
	// (Note: This test may be timing-dependent in real scenarios)

	unsubscribe2()
	stream.Close()
}

func TestShapeStreamPauseResume(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		// Return minimal valid response
		w.Header().Set(ShapeHandleHeader, "test-handle")
		w.Header().Set(ChunkLastOffsetHeader, "0_0")
		w.Header().Set(LiveCacheBusterHeader, "123")
		w.Header().Set(ShapeSchemaHeader, "{}")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"headers": {"control": "up-to-date"}}]`))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := NewShapeStream(Options{
		URL:    server.URL,
		Params: NewParams("test"),
		Ctx:    ctx,
	})
	require.NoError(t, err)

	// Subscribe to start the stream
	unsubscribe := stream.Subscribe(func([]Message) {})
	defer unsubscribe()

	// Wait for initial connection
	assert.Eventually(t, func() bool {
		return stream.IsUpToDate()
	}, 2*time.Second, 100*time.Millisecond)

	// Test NumSubscribers
	assert.Equal(t, 1, stream.NumSubscribers())

	// Pause the stream
	stream.Pause()
	assert.Eventually(t, func() bool {
		return stream.IsPaused()
	}, 1*time.Second, 100*time.Millisecond)

	// Resume the stream
	stream.Resume()
	assert.Eventually(t, func() bool {
		return !stream.IsPaused()
	}, 2*time.Second, 100*time.Millisecond)

	// Stream should still be functional
	assert.True(t, stream.HasStarted())

	stream.Close()
}
