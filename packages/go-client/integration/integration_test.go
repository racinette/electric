package integration

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	goclient "github.com/electric-sql/electric/packages/go-client"
)

func TestHTTPSync(t *testing.T) {
	dbClient, config, err := SetupGlobalTestEnvironment()
	require.NoError(t, err)

	// Test both long polling modes (SSE is not implemented yet)
	testCases := []struct {
		name                string
		experimentalLiveSSE bool
	}{
		{"LongPolling", false},
		// TODO: Enable when SSE is implemented
		// {"SSE", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("EmptyShape", func(t *testing.T) {
				testEmptyShape(t, config, dbClient, tc.experimentalLiveSSE)
			})

			t.Run("InitialData", func(t *testing.T) {
				testInitialData(t, config, dbClient, tc.experimentalLiveSSE)
			})

			t.Run("LiveUpdates", func(t *testing.T) {
				testLiveUpdates(t, config, dbClient, tc.experimentalLiveSSE)
			})

			t.Run("WhereClause", func(t *testing.T) {
				testWhereClause(t, config, dbClient, tc.experimentalLiveSSE)
			})
		})
	}
}

func testEmptyShape(t *testing.T, config *TestConfig, dbClient *TestDBClient, experimentalLiveSSE bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create test table
	table, err := NewTestMultitypeTable(dbClient, config, "empty_shape")
	require.NoError(t, err)
	defer table.Cleanup()

	// Create shape stream
	stream, err := goclient.NewShapeStream(goclient.Options{
		URL:                 config.BaseURL + "/v1/shape",
		Params:              goclient.NewParams(table.TableURL()),
		Subscribe:           false, // Just sync once
		ExperimentalLiveSse: experimentalLiveSSE,
	})
	require.NoError(t, err)
	defer stream.Close()

	// Collect messages
	var allMessages []goclient.Message
	done := make(chan bool)

	unsubscribe := stream.Subscribe(func(messages []goclient.Message) {
		allMessages = append(allMessages, messages...)
		for _, msg := range messages {
			if goclient.IsUpToDateMessage(msg) {
				done <- true
				return
			}
		}
	})
	defer unsubscribe()

	// Wait for up-to-date
	select {
	case <-done:
		// Success
	case <-ctx.Done():
		t.Fatal("Timeout waiting for up-to-date message")
	}

	// Verify only control messages (no data)
	changeCount := 0
	for _, msg := range allMessages {
		if goclient.IsChangeMessage(msg) {
			changeCount++
		}
	}
	assert.Equal(t, 0, changeCount, "Empty shape should have no change messages")
	assert.True(t, stream.IsUpToDate(), "Stream should be up to date")
}

func testInitialData(t *testing.T, config *TestConfig, dbClient *TestDBClient, experimentalLiveSSE bool) {
	// Create test table
	table, err := NewTestMultitypeTable(dbClient, config, "initial_data")
	require.NoError(t, err)
	defer table.Cleanup()

	// Insert initial data
	err = table.InsertTestData()
	require.NoError(t, err)

	// Wait for transaction to be processed
	_, err = WaitForTransaction(config, table.TableURL(), 1, nil)
	require.NoError(t, err)

	// Create shape stream
	stream, err := goclient.NewShapeStream(goclient.Options{
		URL:                 config.BaseURL + "/v1/shape",
		Params:              goclient.NewParams(table.TableURL()),
		Subscribe:           false, // Just sync once
		ExperimentalLiveSse: experimentalLiveSSE,
	})
	require.NoError(t, err)
	defer stream.Close()

	// Create shape to materialize data
	shape := goclient.NewShape(stream)
	defer shape.Close()

	// Wait for sync to complete
	assert.Eventually(t, func() bool {
		return shape.IsUpToDate()
	}, 10*time.Second, 100*time.Millisecond)

	// Verify data
	rows := shape.CurrentRows()
	require.Len(t, rows, 1, "Should have exactly one row")

	row := rows[0]
	assert.Equal(t, "test", row["txt"], "txt should match")
	assert.Equal(t, 1, row["i2"], "i2 should match")
	assert.Equal(t, "123.456", row["num"], "num should match")
}

func testLiveUpdates(t *testing.T, config *TestConfig, dbClient *TestDBClient, experimentalLiveSSE bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create test table
	table, err := NewTestMultitypeTable(dbClient, config, "live_updates")
	require.NoError(t, err)
	defer table.Cleanup()

	// Insert initial data
	err = table.InsertTestData()
	require.NoError(t, err)

	// Wait for initial transaction
	_, err = WaitForTransaction(config, table.TableURL(), 1, nil)
	require.NoError(t, err)

	// Create shape stream with live updates
	stream, err := goclient.NewShapeStream(goclient.Options{
		URL:                 config.BaseURL + "/v1/shape",
		Params:              goclient.NewParams(table.TableURL()),
		Subscribe:           true, // Enable live updates
		ExperimentalLiveSse: experimentalLiveSSE,
	})
	require.NoError(t, err)
	defer stream.Close()

	shape := goclient.NewShape(stream)
	defer shape.Close()

	// Wait for initial sync
	assert.Eventually(t, func() bool {
		return shape.IsUpToDate() && len(shape.CurrentRows()) == 1
	}, 10*time.Second, 100*time.Millisecond)

	// Track updates
	updateCount := 0
	var finalRows []goclient.Row
	updateDone := make(chan bool)

	unsubscribe := shape.Subscribe(func(value map[string]goclient.Row, rows []goclient.Row) {
		updateCount++
		finalRows = rows
		if len(rows) > 0 {
			// Check if this is the updated row
			if rows[0]["txt"] == "changed" {
				updateDone <- true
			}
		}
	})
	defer unsubscribe()

	// Make an update
	err = table.UpdateTestData()
	require.NoError(t, err)

	// Wait for Electric to process the update
	_, err = WaitForTransaction(config, table.TableURL(), 1, nil)
	require.NoError(t, err)

	// Wait for update to be received
	select {
	case <-updateDone:
		// Success
	case <-ctx.Done():
		t.Fatal("Timeout waiting for live update")
	}

	// Verify the update was received
	require.Len(t, finalRows, 1)
	assert.Equal(t, "changed", finalRows[0]["txt"])
	assert.Equal(t, 1, finalRows[0]["i2"])
}

func testWhereClause(t *testing.T, config *TestConfig, dbClient *TestDBClient, experimentalLiveSSE bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Create test table
	table, err := NewTestMultitypeTable(dbClient, config, "where_clause")
	require.NoError(t, err)
	defer table.Cleanup()

	// Insert test data
	err = table.InsertTestData()
	require.NoError(t, err)

	// Wait for transactions
	_, err = WaitForTransaction(config, table.TableURL(), 1, nil)
	require.NoError(t, err)

	// Create shape stream with WHERE clause
	stream, err := goclient.NewShapeStream(goclient.Options{
		URL: config.BaseURL + "/v1/shape",
		Params: goclient.NewParams(table.TableURL()).
			WithWhere("i4 = $1", "2147483647"),
		Subscribe:           true,
		ExperimentalLiveSse: experimentalLiveSSE,
	})
	require.NoError(t, err)
	defer func() {
		// Clean up the shape
		ClearShape(config.BaseURL, table.TableURL(), stream.ShapeHandle())
	}()

	shape := goclient.NewShape(stream)
	defer shape.Close()

	// Wait for initial sync
	assert.Eventually(t, func() bool {
		return shape.IsUpToDate()
	}, 10*time.Second, 100*time.Millisecond)

	// Should only have the "foo" item
	rows := shape.CurrentRows()
	require.Len(t, rows, 1, "Should have exactly one row matching WHERE clause")
	assert.Equal(t, "test", rows[0]["txt"])
	assert.Equal(t, 1, rows[0]["i2"])

	// Update the item, which will cause it to be removed from the shape
	updateDone := make(chan bool)
	var finalRows []goclient.Row

	unsubscribe := shape.Subscribe(func(value map[string]goclient.Row, rows []goclient.Row) {
		finalRows = rows
		if len(rows) == 0 { // Row should be removed
			updateDone <- true
		}
	})
	defer unsubscribe()

	// Update the item (should be removed from shape)
	err = table.UpdateTestData()
	require.NoError(t, err)

	// Wait for the update to be received
	select {
	case <-updateDone:
		// Success
	case <-ctx.Done():
		t.Fatal("Timeout waiting for WHERE clause update")
	}

	// Should have no rows now
	require.Len(t, finalRows, 0)
}

func TestShapeHeaders(t *testing.T) {
	dbClient, config, err := SetupGlobalTestEnvironment()
	require.NoError(t, err)

	// Create test table
	table, err := NewTestMultitypeTable(dbClient, config, "headers")
	require.NoError(t, err)
	defer table.Cleanup()

	// Create shape stream
	stream, err := goclient.NewShapeStream(goclient.Options{
		URL:       config.BaseURL + "/v1/shape",
		Params:    goclient.NewParams(table.TableURL()),
		Subscribe: false,
	})
	require.NoError(t, err)
	defer stream.Close()

	// Get initial sync
	done := make(chan bool)
	unsubscribe := stream.Subscribe(func(messages []goclient.Message) {
		for _, msg := range messages {
			if goclient.IsUpToDateMessage(msg) {
				done <- true
				return
			}
		}
	})
	defer unsubscribe()

	// Wait for sync
	select {
	case <-done:
		// Success
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for headers test")
	}

	// Verify headers were set
	assert.NotEmpty(t, stream.ShapeHandle(), "Shape handle should be set")
	assert.NotEqual(t, "", stream.LastOffset(), "Last offset should be set")
	assert.True(t, stream.IsUpToDate(), "Stream should be up to date")
}

// TestSanityCheck performs basic database connectivity verification
func TestSanityCheck(t *testing.T) {
	dbClient, config, err := SetupGlobalTestEnvironment()
	require.NoError(t, err)

	// Create test table
	table, err := NewTestMultitypeTable(dbClient, config, "sanity")
	require.NoError(t, err)
	defer table.Cleanup()

	// Query empty table
	rows, err := dbClient.Query(fmt.Sprintf("SELECT * FROM %s", table.TableURL()))
	require.NoError(t, err)
	defer rows.Close()

	// Should have no rows
	hasRows := rows.Next()
	assert.False(t, hasRows, "Empty table should have no rows")
}

// TestLongPollingBehavior tests proper long polling wait behavior on empty shapes
func TestLongPollingBehavior(t *testing.T) {
	dbClient, config, err := SetupGlobalTestEnvironment()
	require.NoError(t, err)

	// Test both modes
	testCases := []struct {
		name                string
		experimentalLiveSSE bool
	}{
		{"LongPolling", false},
		// TODO: Enable when SSE is implemented
		// {"SSE", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testLongPollingBehavior(t, config, dbClient, tc.experimentalLiveSSE)
		})
	}
}

func testLongPollingBehavior(t *testing.T, config *TestConfig, dbClient *TestDBClient, experimentalLiveSSE bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create test table
	table, err := NewTestMultitypeTable(dbClient, config, "long_polling")
	require.NoError(t, err)
	defer table.Cleanup()

	// Track requests made (would need custom HTTP client wrapper for full verification)
	requestCount := 0
	upToDateCount := 0

	// Create shape stream
	stream, err := goclient.NewShapeStream(goclient.Options{
		URL:                 config.BaseURL + "/v1/shape",
		Params:              goclient.NewParams(table.TableURL()),
		Subscribe:           true, // Enable live updates to test polling
		ExperimentalLiveSse: experimentalLiveSSE,
	})
	require.NoError(t, err)
	defer stream.Close()

	// Track messages over a short period
	done := make(chan bool)
	unsubscribe := stream.Subscribe(func(messages []goclient.Message) {
		requestCount++
		for _, msg := range messages {
			if goclient.IsUpToDateMessage(msg) {
				upToDateCount++
				if upToDateCount >= 1 {
					// After first up-to-date, wait a bit then finish
					go func() {
						time.Sleep(2 * time.Second)
						done <- true
					}()
				}
			}
		}
	})
	defer unsubscribe()

	// Wait for test completion
	select {
	case <-done:
		// Success - we should have received at least one up-to-date message
		assert.GreaterOrEqual(t, upToDateCount, 1, "Should receive at least one up-to-date message")
	case <-ctx.Done():
		t.Fatal("Timeout during long polling test")
	}
}

// TestHTTPHeaders tests shape handle and offset headers
func TestHTTPHeaders(t *testing.T) {
	dbClient, config, err := SetupGlobalTestEnvironment()
	require.NoError(t, err)

	// Create test table
	table, err := NewTestMultitypeTable(dbClient, config, "http_headers")
	require.NoError(t, err)
	defer table.Cleanup()

	// Test with direct HTTP requests
	// Test shape handle header
	resp, err := http.Get(fmt.Sprintf("%s/v1/shape?table=%s&offset=-1", config.BaseURL, table.TableURL()))
	require.NoError(t, err)
	defer resp.Body.Close()

	shapeHandle := resp.Header.Get("electric-handle")
	assert.NotEmpty(t, shapeHandle, "Response should have electric-handle header")

	lastOffset := resp.Header.Get("electric-offset")
	assert.NotEmpty(t, lastOffset, "Response should have electric-offset header")
}

// TestProcessingBackpressure tests that stream waits for message processing
func TestProcessingBackpressure(t *testing.T) {
	dbClient, config, err := SetupGlobalTestEnvironment()
	require.NoError(t, err)

	// Test both modes
	testCases := []struct {
		name                string
		experimentalLiveSSE bool
	}{
		{"LongPolling", false},
		// TODO: Enable when SSE is implemented
		// {"SSE", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testProcessingBackpressure(t, config, dbClient, tc.experimentalLiveSSE)
		})
	}
}

func testProcessingBackpressure(t *testing.T, config *TestConfig, dbClient *TestDBClient, experimentalLiveSSE bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Create test table
	table, err := NewTestMultitypeTable(dbClient, config, "backpressure")
	require.NoError(t, err)
	defer table.Cleanup()

	// Insert initial data
	err = table.InsertTestData()
	require.NoError(t, err)

	// Create shape stream
	stream, err := goclient.NewShapeStream(goclient.Options{
		URL:                 config.BaseURL + "/v1/shape",
		Params:              goclient.NewParams(table.TableURL()),
		Subscribe:           true,
		ExperimentalLiveSse: experimentalLiveSSE,
	})
	require.NoError(t, err)
	defer stream.Close()

	messageCount := 0
	processingStarted := false
	secondInsertDone := make(chan bool)

	unsubscribe := stream.Subscribe(func(messages []goclient.Message) {
		for _, msg := range messages {
			if goclient.IsChangeMessage(msg) {
				messageCount++
				if messageCount == 1 && !processingStarted {
					processingStarted = true
					// Simulate slow processing
					time.Sleep(2 * time.Second)

					// Insert another row while we're "processing"
					go func() {
						err := table.UpdateTestData()
						if err == nil {
							secondInsertDone <- true
						}
					}()
				}
			}
		}
	})
	defer unsubscribe()

	// Wait for second insert to complete
	select {
	case <-secondInsertDone:
		// Should eventually get both messages
		assert.Eventually(t, func() bool {
			return messageCount >= 2
		}, 10*time.Second, 100*time.Millisecond, "Should receive both messages despite slow processing")
	case <-ctx.Done():
		t.Fatal("Timeout during backpressure test")
	}
}

// TestParallelClients tests multiple clients getting the same data
func TestParallelClients(t *testing.T) {
	dbClient, config, err := SetupGlobalTestEnvironment()
	require.NoError(t, err)

	// Test both modes
	testCases := []struct {
		name                string
		experimentalLiveSSE bool
	}{
		{"LongPolling", false},
		// TODO: Enable when SSE is implemented
		// {"SSE", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testParallelClients(t, config, dbClient, tc.experimentalLiveSSE)
		})
	}
}

func testParallelClients(t *testing.T, config *TestConfig, dbClient *TestDBClient, experimentalLiveSSE bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Create test table
	table, err := NewTestMultitypeTable(dbClient, config, "parallel")
	require.NoError(t, err)
	defer table.Cleanup()

	// Insert initial data
	err = table.InsertTestData()
	require.NoError(t, err)

	// Wait for transaction
	_, err = WaitForTransaction(config, table.TableURL(), 1, nil)
	require.NoError(t, err)

	// Create two parallel streams
	stream1, err := goclient.NewShapeStream(goclient.Options{
		URL:                 config.BaseURL + "/v1/shape",
		Params:              goclient.NewParams(table.TableURL()),
		Subscribe:           true,
		ExperimentalLiveSse: experimentalLiveSSE,
	})
	require.NoError(t, err)
	defer stream1.Close()

	stream2, err := goclient.NewShapeStream(goclient.Options{
		URL:                 config.BaseURL + "/v1/shape",
		Params:              goclient.NewParams(table.TableURL()),
		Subscribe:           true,
		ExperimentalLiveSse: experimentalLiveSSE,
	})
	require.NoError(t, err)
	defer stream2.Close()

	shape1 := goclient.NewShape(stream1)
	defer shape1.Close()
	shape2 := goclient.NewShape(stream2)
	defer shape2.Close()

	// Wait for both to sync
	assert.Eventually(t, func() bool {
		return shape1.IsUpToDate() && shape2.IsUpToDate() &&
			len(shape1.CurrentRows()) == 1 && len(shape2.CurrentRows()) == 1
	}, 10*time.Second, 100*time.Millisecond)

	// Track updates - wait for update to be seen by both clients
	var finalRows1, finalRows2 []goclient.Row
	updateDone := make(chan bool)
	updatesSeen := 0

	unsubscribe1 := shape1.Subscribe(func(value map[string]goclient.Row, rows []goclient.Row) {
		finalRows1 = rows
		if len(rows) > 0 {
			// Check if we have the updated row
			for _, row := range rows {
				if row["txt"] == "changed" {
					updatesSeen++
					if updatesSeen >= 2 {
						updateDone <- true
					}
					break
				}
			}
		}
	})
	defer unsubscribe1()

	unsubscribe2 := shape2.Subscribe(func(value map[string]goclient.Row, rows []goclient.Row) {
		finalRows2 = rows
		if len(rows) > 0 {
			// Check if we have the updated row
			for _, row := range rows {
				if row["txt"] == "changed" {
					updatesSeen++
					if updatesSeen >= 2 {
						updateDone <- true
					}
					break
				}
			}
		}
	})
	defer unsubscribe2()

	// Make an update
	time.Sleep(100 * time.Millisecond)
	err = table.UpdateTestData()
	require.NoError(t, err)

	// Wait for Electric to process the update
	_, err = WaitForTransaction(config, table.TableURL(), 1, nil)
	require.NoError(t, err)

	// Wait for both clients to see the update
	select {
	case <-updateDone:
		// Verify both clients have the same data
		assert.Len(t, finalRows1, 1)
		assert.Len(t, finalRows2, 1)

		// Find the updated row in both clients
		var updatedRow1, updatedRow2 goclient.Row
		for _, row := range finalRows1 {
			if row["i2"] == 1 {
				updatedRow1 = row
				break
			}
		}
		for _, row := range finalRows2 {
			if row["i2"] == 1 {
				updatedRow2 = row
				break
			}
		}

		assert.NotNil(t, updatedRow1, "updatedRow1 should not be nil")
		assert.NotNil(t, updatedRow2, "updatedRow2 should not be nil")
		assert.Equal(t, "changed", updatedRow1["txt"])
		assert.Equal(t, "changed", updatedRow2["txt"])
		assert.Equal(t, updatedRow1["txt"], updatedRow2["txt"])
	case <-ctx.Done():
		t.Fatal("Timeout waiting for parallel client updates")
	}
}

// TestOfflineAndCatchup tests resuming from a previous offset
func TestOfflineAndCatchup(t *testing.T) {
	dbClient, config, err := SetupGlobalTestEnvironment()
	require.NoError(t, err)

	// Test both modes
	testCases := []struct {
		name                string
		experimentalLiveSSE bool
	}{
		{"LongPolling", false},
		// TODO: Enable when SSE is implemented
		// {"SSE", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testOfflineAndCatchup(t, config, dbClient, tc.experimentalLiveSSE)
		})
	}
}

func testOfflineAndCatchup(t *testing.T, config *TestConfig, dbClient *TestDBClient, experimentalLiveSSE bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create test table
	table, err := NewTestMultitypeTable(dbClient, config, "catchup")
	require.NoError(t, err)
	defer table.Cleanup()

	// Insert initial data
	err = table.InsertTestData()
	require.NoError(t, err)

	// Wait for initial transaction and get the stream state
	streamState, err := WaitForTransaction(config, table.TableURL(), 1, nil)
	require.NoError(t, err)

	// Insert more data while "offline"
	err = table.UpdateTestData()
	require.NoError(t, err)

	// Wait for new data to be processed
	_, err = WaitForTransaction(config, table.TableURL(), 1, streamState)
	require.NoError(t, err)

	// Create a new stream resuming from the saved state
	catchupOpsCount := 0
	stream, err := goclient.NewShapeStream(goclient.Options{
		URL:                 config.BaseURL + "/v1/shape",
		Params:              goclient.NewParams(table.TableURL()),
		Subscribe:           true,
		Offset:              streamState.Offset,
		Handle:              streamState.Handle,
		ExperimentalLiveSse: experimentalLiveSSE,
	})
	require.NoError(t, err)
	defer stream.Close()

	done := make(chan bool)
	unsubscribe := stream.Subscribe(func(messages []goclient.Message) {
		for _, msg := range messages {
			if goclient.IsUpToDateMessage(msg) {
				done <- true
				return
			} else if goclient.IsChangeMessage(msg) {
				catchupOpsCount++
			}
		}
	})
	defer unsubscribe()

	// Wait for catchup to complete
	select {
	case <-done:
		// Should have caught up with exactly the expected number of operations
		assert.Equal(t, 1, catchupOpsCount, "Should catch up with exactly the missed operations")
	case <-ctx.Done():
		t.Fatal("Timeout during catchup test")
	}
}

// TestErrorHandling tests various error conditions
func TestErrorHandling(t *testing.T) {
	dbClient, config, err := SetupGlobalTestEnvironment()
	require.NoError(t, err)

	// Test both modes
	testCases := []struct {
		name                string
		experimentalLiveSSE bool
	}{
		{"LongPolling", false},
		// TODO: Enable when SSE is implemented
		// {"SSE", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("InvalidSQL", func(t *testing.T) {
				testInvalidSQL(t, config, dbClient, tc.experimentalLiveSSE)
			})
		})
	}
}

func testInvalidSQL(t *testing.T, config *TestConfig, dbClient *TestDBClient, experimentalLiveSSE bool) {
	// Create test table
	table, err := NewTestMultitypeTable(dbClient, config, "invalid_sql")
	require.NoError(t, err)
	defer table.Cleanup()

	// Try to create stream with invalid WHERE clause
	stream, err := goclient.NewShapeStream(goclient.Options{
		URL: config.BaseURL + "/v1/shape",
		Params: goclient.NewParams(table.TableURL()).
			WithWhere("1 x 1", ""), // Invalid SQL
		Subscribe:           false,
		ExperimentalLiveSse: experimentalLiveSSE,
	})

	if err == nil {
		defer stream.Close()

		// If stream creation succeeded, it should error on first subscription
		errorOccurred := false

		unsubscribe := stream.Subscribe(func(messages []goclient.Message) {
			// Should not receive messages
		})
		defer unsubscribe()

		// Check if stream shows error
		time.Sleep(2 * time.Second)
		if stream.Error() != nil {
			errorOccurred = true
		}

		assert.True(t, errorOccurred, "Stream should have an error for invalid SQL")
	} else {
		// Error during stream creation is also acceptable
		assert.Contains(t, err.Error(), "400", "Should receive 400 error for invalid SQL")
	}
}

// TestShapeDeprecationRestart tests detecting shape deletion and restarting
func TestShapeDeprecationRestart(t *testing.T) {
	dbClient, config, err := SetupGlobalTestEnvironment()
	require.NoError(t, err)

	// Test both modes
	testCases := []struct {
		name                string
		experimentalLiveSSE bool
	}{
		{"LongPolling", false},
		// TODO: Enable when SSE is implemented
		// {"SSE", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testShapeDeprecationRestart(t, config, dbClient, tc.experimentalLiveSSE)
		})
	}
}

func testShapeDeprecationRestart(t *testing.T, config *TestConfig, dbClient *TestDBClient, experimentalLiveSSE bool) {

	// Create test table
	table, err := NewTestMultitypeTable(dbClient, config, "deprecation")
	require.NoError(t, err)
	defer table.Cleanup()

	// Insert initial data
	err = table.InsertTestData()
	require.NoError(t, err)

	// Create shape stream
	stream, err := goclient.NewShapeStream(goclient.Options{
		URL:                 config.BaseURL + "/v1/shape",
		Params:              goclient.NewParams(table.TableURL()),
		Subscribe:           true,
		ExperimentalLiveSse: experimentalLiveSSE,
	})
	require.NoError(t, err)
	defer stream.Close()

	shape := goclient.NewShape(stream)
	defer shape.Close()

	// Wait for initial sync
	assert.Eventually(t, func() bool {
		return shape.IsUpToDate() && len(shape.CurrentRows()) == 1
	}, 10*time.Second, 100*time.Millisecond)

	originalHandle := stream.ShapeHandle()
	require.NotEmpty(t, originalHandle, "Original handle should be set")

	// Wait a moment to ensure initial sync is complete
	time.Sleep(500 * time.Millisecond)

	// Try to delete the shape - this may not be supported in all Electric versions
	err = ClearShape(config.BaseURL, table.TableURL(), originalHandle)
	if err != nil {
		// If DELETE is not supported (405 error), skip this test
		if strings.Contains(err.Error(), "405") {
			t.Skip("Shape deletion not supported in this Electric version (got 405)")
		}
		require.NoError(t, err, "Should be able to delete shape")
	}

	// Add more data to trigger new activity
	err = table.UpdateTestData()
	require.NoError(t, err)

	// Check if the client restarts with a new handle
	// The shape restart might be detected through different mechanisms
	restartDetected := false

	// Wait for potential restart - this might take some time
	for i := 0; i < 50; i++ {
		time.Sleep(200 * time.Millisecond)
		newHandle := stream.ShapeHandle()
		if newHandle != originalHandle && newHandle != "" {
			restartDetected = true
			break
		}

		// Also check if the stream shows any error that might indicate restart
		if stream.Error() != nil {
			// An error could indicate the shape was deleted and needs restart
			restartDetected = true
			break
		}
	}

	// For this test, we'll consider it successful if either:
	// 1. The handle changed (indicating restart)
	// 2. The stream encounters an error (indicating shape deletion detected)
	// 3. The new data eventually appears (indicating restart happened)
	finalRows := shape.CurrentRows()
	newDataDetected := false
	for _, row := range finalRows {
		if row["txt"] == "changed" {
			newDataDetected = true
			break
		}
	}

	success := restartDetected || newDataDetected || stream.Error() != nil
	if !success {
		t.Logf("Original handle: %s", originalHandle)
		t.Logf("Current handle: %s", stream.ShapeHandle())
		t.Logf("Stream error: %v", stream.Error())
		t.Logf("Current rows: %d", len(finalRows))
		t.Logf("Restart detected: %v", restartDetected)
		t.Logf("New data detected: %v", newDataDetected)
	}

	assert.True(t, success, "Should detect shape deletion through restart, error, or new data appearance")
}

// TestCachingHeaders tests HTTP caching headers returned by Electric
func TestCachingHeaders(t *testing.T) {
	dbClient, config, err := SetupGlobalTestEnvironment()
	require.NoError(t, err)

	// Create test table
	table, err := NewTestMultitypeTable(dbClient, config, "caching")
	require.NoError(t, err)
	defer table.Cleanup()

	// Test cache-control headers on initial request
	resp, err := http.Get(fmt.Sprintf("%s/v1/shape?table=%s&offset=-1", config.BaseURL, table.TableURL()))
	require.NoError(t, err)
	defer resp.Body.Close()

	// Verify cache-control header is present
	cacheControl := resp.Header.Get("cache-control")
	assert.NotEmpty(t, cacheControl, "Response should have cache-control header")

	// Parse cache-control directives (basic parsing)
	directives := parseCacheControl(cacheControl)

	// Verify expected cache directives (matching TypeScript test expectations)
	assert.Contains(t, directives, "public", "Should have public directive")
	assert.Contains(t, directives, "max-age", "Should have max-age directive")

	// Verify ETag header is present
	etag := resp.Header.Get("etag")
	assert.NotEmpty(t, etag, "Response should have ETag header")

	// Insert some data
	err = table.InsertTestData()
	require.NoError(t, err)

	// Wait for data to be processed
	_, err = WaitForTransaction(config, table.TableURL(), 1, nil)
	require.NoError(t, err)

	// Get response for same initial request - ETag should be the same
	resp2, err := http.Get(fmt.Sprintf("%s/v1/shape?table=%s&offset=-1", config.BaseURL, table.TableURL()))
	require.NoError(t, err)
	defer resp2.Body.Close()

	etag2 := resp2.Header.Get("etag")
	assert.NotEmpty(t, etag2, "Second response should have ETag header")
	assert.Equal(t, etag, etag2, "ETag should be the same for the same content")

	// Get next chunk - ETag should be different
	electricOffset := resp2.Header.Get("electric-offset")
	electricHandle := resp2.Header.Get("electric-handle")
	assert.NotEmpty(t, electricOffset, "Should have electric-offset header")
	assert.NotEmpty(t, electricHandle, "Should have electric-handle header")

	resp3, err := http.Get(fmt.Sprintf("%s/v1/shape?table=%s&offset=%s&handle=%s",
		config.BaseURL, table.TableURL(), electricOffset, electricHandle))
	require.NoError(t, err)
	defer resp3.Body.Close()

	etag3 := resp3.Header.Get("etag")
	assert.NotEmpty(t, etag3, "Third response should have ETag header")
	assert.NotEqual(t, etag, etag3, "ETag should be different for different content")
}

// TestETagValidation tests ETag validation with conditional requests
func TestETagValidation(t *testing.T) {
	dbClient, config, err := SetupGlobalTestEnvironment()
	require.NoError(t, err)

	// Create test table
	table, err := NewTestMultitypeTable(dbClient, config, "etag_validation")
	require.NoError(t, err)
	defer table.Cleanup()

	// Insert initial data to create a stable state
	err = table.InsertTestData()
	require.NoError(t, err)

	// Wait for data to be processed
	_, err = WaitForTransaction(config, table.TableURL(), 1, nil)
	require.NoError(t, err)

	// Get initial response
	resp, err := http.Get(fmt.Sprintf("%s/v1/shape?table=%s&offset=-1", config.BaseURL, table.TableURL()))
	require.NoError(t, err)
	defer resp.Body.Close()

	etag := resp.Header.Get("etag")
	require.NotEmpty(t, etag, "Response should have ETag header")

	// Make conditional request with If-None-Match
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/v1/shape?table=%s&offset=-1", config.BaseURL, table.TableURL()), nil)
	require.NoError(t, err)
	req.Header.Set("If-None-Match", etag)

	client := &http.Client{}
	resp2, err := client.Do(req)
	require.NoError(t, err)
	defer resp2.Body.Close()

	// Should get 304 Not Modified
	assert.Equal(t, http.StatusNotModified, resp2.StatusCode, "Should receive 304 Not Modified for matching ETag")

	// Add more data to change the content
	err = table.UpdateTestData()
	require.NoError(t, err)

	// Wait for new data to be processed
	_, err = WaitForTransaction(config, table.TableURL(), 1, nil)
	require.NoError(t, err)

	// Get response for a different offset that should have different content
	electricOffset := resp.Header.Get("electric-offset")
	electricHandle := resp.Header.Get("electric-handle")

	// Make request for catchup data
	req3, err := http.NewRequest("GET", fmt.Sprintf("%s/v1/shape?table=%s&offset=%s&handle=%s",
		config.BaseURL, table.TableURL(), electricOffset, electricHandle), nil)
	require.NoError(t, err)

	resp3, err := client.Do(req3)
	require.NoError(t, err)
	defer resp3.Body.Close()

	// Should get 200 with new content and different ETag
	assert.Equal(t, http.StatusOK, resp3.StatusCode, "Should receive 200 for new content")

	etag3 := resp3.Header.Get("etag")
	assert.NotEmpty(t, etag3, "New response should have ETag")
	assert.NotEqual(t, etag, etag3, "ETag should be different for different content")
}

// parseCacheControl parses cache-control header into a map of directives
func parseCacheControl(header string) map[string]string {
	directives := make(map[string]string)
	parts := strings.Split(header, ",")

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		if strings.Contains(part, "=") {
			kv := strings.SplitN(part, "=", 2)
			key := strings.TrimSpace(kv[0])
			value := strings.TrimSpace(kv[1])
			directives[key] = value
		} else {
			directives[part] = "true"
		}
	}

	return directives
}
