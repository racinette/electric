package integration

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
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
	table, err := NewTestIssuesTable(dbClient, config, "empty_shape")
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
	table, err := NewTestIssuesTable(dbClient, config, "initial_data")
	require.NoError(t, err)
	defer table.Cleanup()

	// Insert initial data
	testID := uuid.New().String()
	testTitle := "Test Issue " + testID
	ids, err := table.InsertIssues(IssueRow{
		ID:    testID,
		Title: testTitle,
	})
	require.NoError(t, err)
	require.Len(t, ids, 1)

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
	assert.Equal(t, testID, row["id"], "ID should match")
	assert.Equal(t, testTitle, row["title"], "Title should match")
	assert.Equal(t, 10, row["priority"], "Priority should be default 10")
}

func testLiveUpdates(t *testing.T, config *TestConfig, dbClient *TestDBClient, experimentalLiveSSE bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create test table
	table, err := NewTestIssuesTable(dbClient, config, "live_updates")
	require.NoError(t, err)
	defer table.Cleanup()

	// Insert initial data
	initialID := uuid.New().String()
	ids, err := table.InsertIssues(IssueRow{
		ID:    initialID,
		Title: "Original Title",
	})
	require.NoError(t, err)
	require.Len(t, ids, 1)

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
			if rows[0]["title"] == "Updated Title" {
				updateDone <- true
			}
		}
	})
	defer unsubscribe()

	// Make an update
	err = table.UpdateIssue(initialID, IssueRow{Title: "Updated Title"})
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
	assert.Equal(t, "Updated Title", finalRows[0]["title"])
	assert.Equal(t, initialID, finalRows[0]["id"])
}

func testWhereClause(t *testing.T, config *TestConfig, dbClient *TestDBClient, experimentalLiveSSE bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Create test table
	table, err := NewTestIssuesTable(dbClient, config, "where_clause")
	require.NoError(t, err)
	defer table.Cleanup()

	// Insert test data
	id1 := uuid.New().String()
	id2 := uuid.New().String()
	_, err = table.InsertIssues(
		IssueRow{ID: id1, Title: "foo item"},
		IssueRow{ID: id2, Title: "bar item"},
	)
	require.NoError(t, err)

	// Wait for transactions
	_, err = WaitForTransaction(config, table.TableURL(), 2, nil)
	require.NoError(t, err)

	// Create shape stream with WHERE clause
	stream, err := goclient.NewShapeStream(goclient.Options{
		URL: config.BaseURL + "/v1/shape",
		Params: goclient.NewParams(table.TableURL()).
			WithWhere("title LIKE $1", "foo%"),
		Subscribe:           true,
		ExperimentalLiveSse: experimentalLiveSSE,
	})
	require.NoError(t, err)
	defer stream.Close()
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
	assert.Equal(t, "foo item", rows[0]["title"])
	assert.Equal(t, id1, rows[0]["id"])

	// Update both items
	updateDone := make(chan bool)
	var finalRows []goclient.Row

	unsubscribe := shape.Subscribe(func(value map[string]goclient.Row, rows []goclient.Row) {
		finalRows = rows
		if len(rows) == 1 && rows[0]["title"] == "foo updated" {
			updateDone <- true
		}
	})
	defer unsubscribe()

	// Update the foo item (should be visible)
	err = table.UpdateIssue(id1, IssueRow{Title: "foo updated"})
	require.NoError(t, err)

	// Update the bar item (should not be visible due to WHERE clause)
	err = table.UpdateIssue(id2, IssueRow{Title: "bar updated"})
	require.NoError(t, err)

	// Wait for the foo update to be received
	select {
	case <-updateDone:
		// Success
	case <-ctx.Done():
		t.Fatal("Timeout waiting for WHERE clause update")
	}

	// Should still only have one row (the foo item)
	require.Len(t, finalRows, 1)
	assert.Equal(t, "foo updated", finalRows[0]["title"])
	assert.Equal(t, id1, finalRows[0]["id"])
}

func TestShapeHeaders(t *testing.T) {
	dbClient, config, err := SetupGlobalTestEnvironment()
	require.NoError(t, err)

	// Create test table
	table, err := NewTestIssuesTable(dbClient, config, "headers")
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
