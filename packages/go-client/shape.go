package goclient

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Shape materializes data from a ShapeStream into an in-memory map keyed by message key.
type Shape struct {
	stream *ShapeStream

	// Atomic fields
	subscriptionCounter int64

	// Protected by mutex
	mu             sync.RWMutex
	data           map[string]Row
	subscribers    map[int64]func(value map[string]Row, rows []Row, diff Diff)
	statusUpToDate bool
	lastErr        error

	// Diff tracking between notification points
	currentDiff Diff

	// Message serialization to prevent race conditions
	notificationQueue  chan struct{}
	notificationCancel context.CancelFunc
}

func NewShape(stream *ShapeStream) *Shape {
	// Create context for notification queue
	notificationCtx, notificationCancel := context.WithCancel(context.Background())

	s := &Shape{
		stream:             stream,
		data:               make(map[string]Row),
		subscribers:        make(map[int64]func(map[string]Row, []Row, Diff)),
		notificationQueue:  make(chan struct{}, 100), // Buffered channel for notifications
		notificationCancel: notificationCancel,
		currentDiff: Diff{
			Updates: make(map[string]Update),
			Deletes: make(map[string]Delete),
			Inserts: make(map[string]Insert),
		},
	}

	// Start notification worker
	go s.notificationWorker(notificationCtx)

	stream.Subscribe(s.process)
	return s
}

func (s *Shape) process(messages []Message) {
	s.mu.Lock()
	defer s.mu.Unlock()

	shouldNotify := false

	for _, m := range messages {
		if IsChangeMessage(m) {
			s.processChangeMessage(m.Change)
			// Update to syncing status on any data change
			shouldNotify = s.updateShapeStatus(false)
		} else if IsControlMessage(m) {
			switch m.Control.Headers.Control {
			case "up-to-date":
				shouldNotify = s.updateShapeStatus(true)
			case "must-refetch":
				s.data = make(map[string]Row)
				s.lastErr = nil
				s.resetDiff()
				shouldNotify = s.updateShapeStatus(false)
			}
		}
	}

	if shouldNotify {
		s.notifySubscribers()
	}
}

// processChangeMessage processes a single change message and updates both data and diff
func (s *Shape) processChangeMessage(change *ChangeMessage) {
	key := change.Key

	switch change.Headers.Operation {
	case OpInsert:
		s.processInsert(key, change.Value)
	case OpUpdate:
		s.processUpdate(key, change.Value)
	case OpDelete:
		s.processDelete(key, change.OldValue)
	}
}

// processInsert handles insert operations with flattening logic
func (s *Shape) processInsert(key string, newRow Row) {
	// Update the main data
	s.data[key] = newRow

	// Check if there's already an operation for this key in the current diff
	if _, hasUpdate := s.currentDiff.Updates[key]; hasUpdate {
		// If there was an update, remove it and replace with insert
		delete(s.currentDiff.Updates, key)
		s.currentDiff.Inserts[key] = Insert{Key: key, New: newRow}
	} else if _, hasDelete := s.currentDiff.Deletes[key]; hasDelete {
		// If there was a delete, remove it and replace with insert
		delete(s.currentDiff.Deletes, key)
		s.currentDiff.Inserts[key] = Insert{Key: key, New: newRow}
	} else if _, hasInsert := s.currentDiff.Inserts[key]; hasInsert {
		// If there's already an insert, update it with the new data (flattened)
		s.currentDiff.Inserts[key] = Insert{Key: key, New: newRow}
	} else {
		// No existing operation, add new insert
		s.currentDiff.Inserts[key] = Insert{Key: key, New: newRow}
	}
}

// processUpdate handles update operations with flattening logic
func (s *Shape) processUpdate(key string, updates Row) {
	// Handle partial updates by merging with existing data
	existing := s.data[key]
	if existing == nil {
		existing = make(Row)
	}

	// Create new row by merging updates
	newRow := make(Row)
	for k, v := range existing {
		newRow[k] = v
	}
	for k, v := range updates {
		newRow[k] = v
	}

	// Update the main data
	s.data[key] = newRow

	// Check diff operations for this key
	if existingInsert, hasInsert := s.currentDiff.Inserts[key]; hasInsert {
		// If there's an insert, merge the update into it (flattened)
		mergedRow := make(Row)
		for k, v := range existingInsert.New {
			mergedRow[k] = v
		}
		for k, v := range updates {
			mergedRow[k] = v
		}
		s.currentDiff.Inserts[key] = Insert{Key: key, New: mergedRow}
	} else if existingUpdate, hasUpdate := s.currentDiff.Updates[key]; hasUpdate {
		// If there's already an update, merge the diffs and update old/new values
		mergedDiff := make(Row)
		for k, v := range existingUpdate.Diff {
			mergedDiff[k] = v
		}
		for k, v := range updates {
			mergedDiff[k] = v
		}

		s.currentDiff.Updates[key] = Update{
			Key:  key,
			Old:  existingUpdate.Old, // Keep the original old value
			New:  newRow,
			Diff: mergedDiff,
		}
	} else {
		// No existing operation, create new update
		// Get the old value from current data before this update
		oldRow := make(Row)
		for k, v := range existing {
			oldRow[k] = v
		}

		s.currentDiff.Updates[key] = Update{
			Key:  key,
			Old:  oldRow,
			New:  newRow,
			Diff: updates,
		}
	}
}

// processDelete handles delete operations with flattening logic
func (s *Shape) processDelete(key string, deletedRow Row) {
	// Get the current value before deletion for the Deleted field
	currentValue := s.data[key]
	if currentValue == nil && deletedRow != nil {
		currentValue = deletedRow
	}

	// Delete from main data
	delete(s.data, key)

	// Handle diff operations
	if _, hasInsert := s.currentDiff.Inserts[key]; hasInsert {
		// If there was an insert, remove both insert and don't add delete
		// (net effect: neither insert nor delete happened)
		delete(s.currentDiff.Inserts, key)
	} else {
		// Remove any existing update and add delete
		delete(s.currentDiff.Updates, key)

		// Use the value that was actually in the data before deletion
		var actualDeleted Row
		if currentValue != nil {
			actualDeleted = currentValue
		} else if deletedRow != nil {
			actualDeleted = deletedRow
		} else {
			actualDeleted = make(Row)
		}

		// For replica identity, use deletedRow if available, otherwise use the actual deleted value
		replicaIdentity := deletedRow
		if replicaIdentity == nil {
			replicaIdentity = actualDeleted
		}

		s.currentDiff.Deletes[key] = Delete{
			Key:             key,
			Deleted:         actualDeleted,
			ReplicaIdentity: replicaIdentity,
		}
	}
}

// resetDiff clears the current diff maps
func (s *Shape) resetDiff() {
	s.currentDiff.Updates = make(map[string]Update)
	s.currentDiff.Deletes = make(map[string]Delete)
	s.currentDiff.Inserts = make(map[string]Insert)
}

func (s *Shape) updateShapeStatus(upToDate bool) bool {
	stateChanged := s.statusUpToDate != upToDate
	s.statusUpToDate = upToDate
	return stateChanged && upToDate
}

func (s *Shape) notifySubscribers() {
	s.notificationQueue <- struct{}{}
}

func (s *Shape) Subscribe(cb func(value map[string]Row, rows []Row, diff Diff)) (unsubscribe func()) {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := atomic.AddInt64(&s.subscriptionCounter, 1)
	s.subscribers[id] = cb

	return func() {
		s.mu.Lock()
		delete(s.subscribers, id)
		s.mu.Unlock()
	}
}

func (s *Shape) snapshotLocked() map[string]Row {
	cp := make(map[string]Row, len(s.data))
	for k, v := range s.data {
		cp[k] = v
	}
	return cp
}

func (s *Shape) Rows() []Row {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows := make([]Row, 0, len(s.data))
	for _, v := range s.data {
		rows = append(rows, v)
	}
	return rows
}

func (s *Shape) CurrentRows() []Row {
	return s.Rows()
}

func (s *Shape) CurrentValue() map[string]Row {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshotLocked()
}

func (s *Shape) IsUpToDate() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.statusUpToDate
}

func (s *Shape) LastOffset() Offset {
	return s.stream.LastOffset()
}

func (s *Shape) Handle() string {
	return s.stream.ShapeHandle()
}

func (s *Shape) LastSyncedAt() *time.Time {
	return s.stream.LastSyncedAt()
}

func (s *Shape) LastSynced() time.Duration {
	return s.stream.LastSynced()
}

func (s *Shape) IsLoading() bool {
	return s.stream.IsLoading()
}

func (s *Shape) IsConnected() bool {
	return s.stream.IsConnected()
}

func (s *Shape) Error() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.lastErr != nil {
		return s.lastErr
	}
	return s.stream.Error()
}

func (s *Shape) UnsubscribeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscribers = make(map[int64]func(map[string]Row, []Row, Diff))
}

func (s *Shape) NumSubscribers() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.subscribers)
}

// Pause temporarily stops the underlying stream
func (s *Shape) Pause() {
	s.stream.Pause()
}

// Resume resumes the underlying stream if it was paused
func (s *Shape) Resume() {
	s.stream.Resume()
}

// notificationWorker processes notifications sequentially to prevent race conditions
func (s *Shape) notificationWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.notificationQueue:
			// Process notification sequentially
			s.callSubscribersSync()
		}
	}
}

// callSubscribersSync calls all subscribers synchronously with current shape data
func (s *Shape) callSubscribersSync() {
	s.mu.RLock()
	// Get snapshot and subscribers while locked
	value := s.snapshotLocked()
	rows := make([]Row, 0, len(value))
	for _, v := range value {
		rows = append(rows, v)
	}

	// Create copy of current diff for subscribers
	diffCopy := Diff{
		Updates: make(map[string]Update, len(s.currentDiff.Updates)),
		Deletes: make(map[string]Delete, len(s.currentDiff.Deletes)),
		Inserts: make(map[string]Insert, len(s.currentDiff.Inserts)),
	}
	for k, v := range s.currentDiff.Updates {
		diffCopy.Updates[k] = v
	}
	for k, v := range s.currentDiff.Deletes {
		diffCopy.Deletes[k] = v
	}
	for k, v := range s.currentDiff.Inserts {
		diffCopy.Inserts[k] = v
	}

	subs := make([]func(map[string]Row, []Row, Diff), 0, len(s.subscribers))
	for _, cb := range s.subscribers {
		subs = append(subs, cb)
	}
	s.mu.RUnlock()

	// Call all subscribers for this notification sequentially
	for _, cb := range subs {
		func(callback func(map[string]Row, []Row, Diff)) {
			defer func() {
				if r := recover(); r != nil {
					fmt.Println("panic in subscriber", r)
				}
			}()
			callback(value, rows, diffCopy)
		}(cb)
	}

	// Reset diff after all subscribers have been notified
	s.mu.Lock()
	s.resetDiff()
	s.mu.Unlock()
}

// Close closes the underlying stream and cleans up resources
func (s *Shape) Close() {
	// Stop notification worker
	if s.notificationCancel != nil {
		s.notificationCancel()
	}

	s.stream.Close()
	s.UnsubscribeAll()
}
