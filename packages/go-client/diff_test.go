package goclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDiffFlattening tests the core diff flattening logic
func TestDiffFlattening(t *testing.T) {
	t.Run("InsertOnly", func(t *testing.T) {
		stream, _ := NewShapeStream(Options{
			URL:    "http://localhost:3000/v1/shape",
			Params: NewParams("test"),
		})
		shape := NewShape(stream)

		// Simulate insert message
		insertMsg := Message{
			Change: &ChangeMessage{
				Key:   "key1",
				Value: Row{"id": "key1", "name": "John"},
				Headers: struct {
					Operation Operation `json:"operation"`
				}{Operation: OpInsert},
			},
		}

		upToDateMsg := Message{
			Control: &ControlMessage{
				Headers: struct {
					Control           string `json:"control"`
					GlobalLastSeenLSN string `json:"global_last_seen_lsn,omitempty"`
				}{Control: "up-to-date"},
			},
		}

		var receivedDiff Diff
		var wg sync.WaitGroup
		wg.Add(1)

		unsubscribe := shape.Subscribe(func(value map[string]Row, rows []Row, diff Diff) {
			receivedDiff = diff
			wg.Done()
		})
		defer unsubscribe()

		// Process messages
		shape.process([]Message{insertMsg, upToDateMsg})

		// Wait for notification
		wg.Wait()

		// Check diff
		assert.Len(t, receivedDiff.Inserts, 1)
		assert.Len(t, receivedDiff.Updates, 0)
		assert.Len(t, receivedDiff.Deletes, 0)

		insert := receivedDiff.Inserts["key1"]
		assert.Equal(t, "key1", insert.Key)
		assert.Equal(t, "key1", insert.New["id"])
		assert.Equal(t, "John", insert.New["name"])
	})

	t.Run("UpdateOnInsert", func(t *testing.T) {
		stream, _ := NewShapeStream(Options{
			URL:    "http://localhost:3000/v1/shape",
			Params: NewParams("test"),
		})
		shape := NewShape(stream)

		// Simulate insert followed by update
		insertMsg := Message{
			Change: &ChangeMessage{
				Key:   "key1",
				Value: Row{"id": "key1", "name": "John"},
				Headers: struct {
					Operation Operation `json:"operation"`
				}{Operation: OpInsert},
			},
		}

		updateMsg := Message{
			Change: &ChangeMessage{
				Key:   "key1",
				Value: Row{"name": "Jane"}, // Partial update
				Headers: struct {
					Operation Operation `json:"operation"`
				}{Operation: OpUpdate},
			},
		}

		upToDateMsg := Message{
			Control: &ControlMessage{
				Headers: struct {
					Control           string `json:"control"`
					GlobalLastSeenLSN string `json:"global_last_seen_lsn,omitempty"`
				}{Control: "up-to-date"},
			},
		}

		var receivedDiff Diff
		var wg sync.WaitGroup
		wg.Add(1)

		unsubscribe := shape.Subscribe(func(value map[string]Row, rows []Row, diff Diff) {
			receivedDiff = diff
			wg.Done()
		})
		defer unsubscribe()

		// Process messages
		shape.process([]Message{insertMsg, updateMsg, upToDateMsg})

		// Wait for notification
		wg.Wait()

		// Check diff - should be flattened into a single insert
		assert.Len(t, receivedDiff.Inserts, 1)
		assert.Len(t, receivedDiff.Updates, 0)
		assert.Len(t, receivedDiff.Deletes, 0)

		insert := receivedDiff.Inserts["key1"]
		assert.Equal(t, "key1", insert.Key)
		assert.Equal(t, "key1", insert.New["id"])
		assert.Equal(t, "Jane", insert.New["name"]) // Updated value
	})

	t.Run("DeleteOnInsert", func(t *testing.T) {
		stream, _ := NewShapeStream(Options{
			URL:    "http://localhost:3000/v1/shape",
			Params: NewParams("test"),
		})
		shape := NewShape(stream)

		// Simulate insert followed by delete
		insertMsg := Message{
			Change: &ChangeMessage{
				Key:   "key1",
				Value: Row{"id": "key1", "name": "John"},
				Headers: struct {
					Operation Operation `json:"operation"`
				}{Operation: OpInsert},
			},
		}

		deleteMsg := Message{
			Change: &ChangeMessage{
				Key:      "key1",
				OldValue: Row{"id": "key1", "name": "John"},
				Headers: struct {
					Operation Operation `json:"operation"`
				}{Operation: OpDelete},
			},
		}

		upToDateMsg := Message{
			Control: &ControlMessage{
				Headers: struct {
					Control           string `json:"control"`
					GlobalLastSeenLSN string `json:"global_last_seen_lsn,omitempty"`
				}{Control: "up-to-date"},
			},
		}

		var receivedDiff Diff
		var wg sync.WaitGroup
		wg.Add(1)

		unsubscribe := shape.Subscribe(func(value map[string]Row, rows []Row, diff Diff) {
			receivedDiff = diff
			wg.Done()
		})
		defer unsubscribe()

		// Process messages
		shape.process([]Message{insertMsg, deleteMsg, upToDateMsg})

		// Wait for notification
		wg.Wait()

		// Check diff - should be empty (insert + delete = nothing happened)
		assert.Len(t, receivedDiff.Inserts, 0)
		assert.Len(t, receivedDiff.Updates, 0)
		assert.Len(t, receivedDiff.Deletes, 0)
	})

	t.Run("UpdateOnExistingRow", func(t *testing.T) {
		stream, _ := NewShapeStream(Options{
			URL:    "http://localhost:3000/v1/shape",
			Params: NewParams("test"),
		})
		shape := NewShape(stream)

		// Pre-populate data
		shape.data["key1"] = Row{"id": "key1", "name": "John", "age": 30}

		// Simulate update on existing row
		updateMsg := Message{
			Change: &ChangeMessage{
				Key:   "key1",
				Value: Row{"name": "Jane"}, // Partial update
				Headers: struct {
					Operation Operation `json:"operation"`
				}{Operation: OpUpdate},
			},
		}

		upToDateMsg := Message{
			Control: &ControlMessage{
				Headers: struct {
					Control           string `json:"control"`
					GlobalLastSeenLSN string `json:"global_last_seen_lsn,omitempty"`
				}{Control: "up-to-date"},
			},
		}

		var receivedDiff Diff
		var wg sync.WaitGroup
		wg.Add(1)

		unsubscribe := shape.Subscribe(func(value map[string]Row, rows []Row, diff Diff) {
			receivedDiff = diff
			wg.Done()
		})
		defer unsubscribe()

		// Process messages
		shape.process([]Message{updateMsg, upToDateMsg})

		// Wait for notification
		wg.Wait()

		// Check diff
		assert.Len(t, receivedDiff.Inserts, 0)
		assert.Len(t, receivedDiff.Updates, 1)
		assert.Len(t, receivedDiff.Deletes, 0)

		update := receivedDiff.Updates["key1"]
		assert.Equal(t, "key1", update.Key)
		assert.Equal(t, "John", update.Old["name"])
		assert.Equal(t, 30, update.Old["age"])
		assert.Equal(t, "Jane", update.New["name"])
		assert.Equal(t, 30, update.New["age"]) // Should remain unchanged
		assert.Equal(t, "Jane", update.Diff["name"])
		assert.NotContains(t, update.Diff, "age") // age wasn't updated
	})

	t.Run("MultipleUpdatesFlattened", func(t *testing.T) {
		stream, _ := NewShapeStream(Options{
			URL:    "http://localhost:3000/v1/shape",
			Params: NewParams("test"),
		})
		shape := NewShape(stream)

		// Pre-populate data
		shape.data["key1"] = Row{"id": "key1", "name": "John", "age": 30}

		// Simulate multiple updates
		updateMsg1 := Message{
			Change: &ChangeMessage{
				Key:   "key1",
				Value: Row{"name": "Jane"},
				Headers: struct {
					Operation Operation `json:"operation"`
				}{Operation: OpUpdate},
			},
		}

		updateMsg2 := Message{
			Change: &ChangeMessage{
				Key:   "key1",
				Value: Row{"age": 31},
				Headers: struct {
					Operation Operation `json:"operation"`
				}{Operation: OpUpdate},
			},
		}

		upToDateMsg := Message{
			Control: &ControlMessage{
				Headers: struct {
					Control           string `json:"control"`
					GlobalLastSeenLSN string `json:"global_last_seen_lsn,omitempty"`
				}{Control: "up-to-date"},
			},
		}

		var receivedDiff Diff
		var wg sync.WaitGroup
		wg.Add(1)

		unsubscribe := shape.Subscribe(func(value map[string]Row, rows []Row, diff Diff) {
			receivedDiff = diff
			wg.Done()
		})
		defer unsubscribe()

		// Process messages
		shape.process([]Message{updateMsg1, updateMsg2, upToDateMsg})

		// Wait for notification
		wg.Wait()

		// Check diff - should be flattened into single update
		assert.Len(t, receivedDiff.Inserts, 0)
		assert.Len(t, receivedDiff.Updates, 1)
		assert.Len(t, receivedDiff.Deletes, 0)

		update := receivedDiff.Updates["key1"]
		assert.Equal(t, "key1", update.Key)
		assert.Equal(t, "John", update.Old["name"])
		assert.Equal(t, 30, update.Old["age"])
		assert.Equal(t, "Jane", update.New["name"])
		assert.Equal(t, 31, update.New["age"])
		assert.Equal(t, "Jane", update.Diff["name"])
		assert.Equal(t, 31, update.Diff["age"])
	})

	t.Run("DeleteOnUpdate", func(t *testing.T) {
		stream, _ := NewShapeStream(Options{
			URL:    "http://localhost:3000/v1/shape",
			Params: NewParams("test"),
		})
		shape := NewShape(stream)

		// Pre-populate data
		shape.data["key1"] = Row{"id": "key1", "name": "John", "age": 30}

		// Simulate update followed by delete
		updateMsg := Message{
			Change: &ChangeMessage{
				Key:   "key1",
				Value: Row{"name": "Jane"},
				Headers: struct {
					Operation Operation `json:"operation"`
				}{Operation: OpUpdate},
			},
		}

		deleteMsg := Message{
			Change: &ChangeMessage{
				Key:      "key1",
				OldValue: Row{"id": "key1", "name": "Jane", "age": 30},
				Headers: struct {
					Operation Operation `json:"operation"`
				}{Operation: OpDelete},
			},
		}

		upToDateMsg := Message{
			Control: &ControlMessage{
				Headers: struct {
					Control           string `json:"control"`
					GlobalLastSeenLSN string `json:"global_last_seen_lsn,omitempty"`
				}{Control: "up-to-date"},
			},
		}

		var receivedDiff Diff
		var wg sync.WaitGroup
		wg.Add(1)

		unsubscribe := shape.Subscribe(func(value map[string]Row, rows []Row, diff Diff) {
			receivedDiff = diff
			wg.Done()
		})
		defer unsubscribe()

		// Process messages
		shape.process([]Message{updateMsg, deleteMsg, upToDateMsg})

		// Wait for notification
		wg.Wait()

		// Check diff - should only have delete (update was cancelled by delete)
		assert.Len(t, receivedDiff.Inserts, 0)
		assert.Len(t, receivedDiff.Updates, 0)
		assert.Len(t, receivedDiff.Deletes, 1)

		delete := receivedDiff.Deletes["key1"]
		assert.Equal(t, "key1", delete.Key)
		assert.Equal(t, "Jane", delete.Deleted["name"]) // Should have the updated value that was deleted
		assert.Equal(t, 30, delete.Deleted["age"])
	})
}

func TestDiffReset(t *testing.T) {
	t.Run("MustRefetchResetsDiff", func(t *testing.T) {
		stream, _ := NewShapeStream(Options{
			URL:    "http://localhost:3000/v1/shape",
			Params: NewParams("test"),
		})
		shape := NewShape(stream)

		// Simulate some changes first
		insertMsg := Message{
			Change: &ChangeMessage{
				Key:   "key1",
				Value: Row{"id": "key1", "name": "John"},
				Headers: struct {
					Operation Operation `json:"operation"`
				}{Operation: OpInsert},
			},
		}

		mustRefetchMsg := Message{
			Control: &ControlMessage{
				Headers: struct {
					Control           string `json:"control"`
					GlobalLastSeenLSN string `json:"global_last_seen_lsn,omitempty"`
				}{Control: "must-refetch"},
			},
		}

		upToDateMsg := Message{
			Control: &ControlMessage{
				Headers: struct {
					Control           string `json:"control"`
					GlobalLastSeenLSN string `json:"global_last_seen_lsn,omitempty"`
				}{Control: "up-to-date"},
			},
		}

		var allDiffs []Diff
		var mu sync.Mutex
		notificationCh := make(chan bool, 2)

		unsubscribe := shape.Subscribe(func(value map[string]Row, rows []Row, diff Diff) {
			mu.Lock()
			allDiffs = append(allDiffs, diff)
			mu.Unlock()
			notificationCh <- true
		})
		defer unsubscribe()

		// Process first batch with insert
		shape.process([]Message{insertMsg, upToDateMsg})

		// Wait for first notification
		select {
		case <-notificationCh:
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for first notification")
		}

		// Process must-refetch
		shape.process([]Message{mustRefetchMsg, upToDateMsg})

		// Wait for second notification
		select {
		case <-notificationCh:
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for second notification")
		}

		// Check results
		mu.Lock()
		require.Len(t, allDiffs, 2)

		// First diff should have the insert
		assert.Len(t, allDiffs[0].Inserts, 1)

		// Second diff should be empty after must-refetch
		assert.Len(t, allDiffs[1].Inserts, 0)
		assert.Len(t, allDiffs[1].Updates, 0)
		assert.Len(t, allDiffs[1].Deletes, 0)
		mu.Unlock()
	})
}

// TestDiffIntegrationWithMockServer tests diff functionality with a more realistic server setup
func TestDiffIntegrationWithMockServer(t *testing.T) {
	// Create a mock server that can simulate a sequence of operations
	messageSequence := []interface{}{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(ShapeHandleHeader, "test-handle")
		w.Header().Set(ChunkLastOffsetHeader, "1_0")
		w.Header().Set(LiveCacheBusterHeader, "123")
		w.Header().Set(ShapeSchemaHeader, `{"id": {"type": "text"}, "name": {"type": "text"}}`)

		// Return the pre-configured message sequence
		messages := append(messageSequence, map[string]interface{}{
			"headers": map[string]interface{}{
				"control": "up-to-date",
			},
		})

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(messages)
	}))
	defer server.Close()

	t.Run("SequentialOperations", func(t *testing.T) {
		// Set up message sequence: insert -> update -> delete
		messageSequence = []interface{}{
			map[string]interface{}{
				"key":   "test-key",
				"value": map[string]interface{}{"id": "test-key", "name": "John"},
				"headers": map[string]interface{}{
					"operation": "insert",
				},
			},
			map[string]interface{}{
				"key":   "test-key",
				"value": map[string]interface{}{"name": "Jane"},
				"headers": map[string]interface{}{
					"operation": "update",
				},
			},
			map[string]interface{}{
				"key":       "test-key",
				"old_value": map[string]interface{}{"id": "test-key", "name": "Jane"},
				"headers": map[string]interface{}{
					"operation": "delete",
				},
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		stream, err := NewShapeStream(Options{
			URL:    server.URL,
			Params: NewParams("test"),
			Ctx:    ctx,
		})
		require.NoError(t, err)

		shape := NewShape(stream)

		var receivedDiff Diff
		diffReceived := make(chan bool, 1)

		unsubscribe := shape.Subscribe(func(value map[string]Row, rows []Row, diff Diff) {
			receivedDiff = diff
			select {
			case diffReceived <- true:
			default:
			}
		})
		defer unsubscribe()

		// Wait for diff
		select {
		case <-diffReceived:
		case <-time.After(3 * time.Second):
			t.Fatal("Timeout waiting for diff")
		}

		// The sequence insert -> update -> delete should result in no operations
		// (insert cancelled by delete, update irrelevant)
		assert.Len(t, receivedDiff.Inserts, 0)
		assert.Len(t, receivedDiff.Updates, 0)
		assert.Len(t, receivedDiff.Deletes, 0)

		stream.Close()
	})
}
