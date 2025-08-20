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
	subscribers    map[int64]func(value map[string]Row, rows []Row)
	statusUpToDate bool
	lastErr        error

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
		subscribers:        make(map[int64]func(map[string]Row, []Row)),
		notificationQueue:  make(chan struct{}, 100), // Buffered channel for notifications
		notificationCancel: notificationCancel,
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
			switch m.Change.Headers.Operation {
			case OpInsert:
				s.data[m.Change.Key] = m.Change.Value
			case OpUpdate:
				// Handle partial updates by merging with existing data
				existing := s.data[m.Change.Key]
				if existing == nil {
					existing = make(Row)
				}
				for k, v := range m.Change.Value {
					existing[k] = v
				}
				s.data[m.Change.Key] = existing
			case OpDelete:
				delete(s.data, m.Change.Key)
			}
			// Update to syncing status on any data change
			shouldNotify = s.updateShapeStatus(false)
		} else if IsControlMessage(m) {
			switch m.Control.Headers.Control {
			case "up-to-date":
				shouldNotify = s.updateShapeStatus(true)
			case "must-refetch":
				s.data = make(map[string]Row)
				s.lastErr = nil
				shouldNotify = s.updateShapeStatus(false)
			}
		}
	}

	if shouldNotify {
		s.notifySubscribers()
	}
}

func (s *Shape) updateShapeStatus(upToDate bool) bool {
	stateChanged := s.statusUpToDate != upToDate
	s.statusUpToDate = upToDate
	return stateChanged && upToDate
}

func (s *Shape) notifySubscribers() {
	s.notificationQueue <- struct{}{}
}

func (s *Shape) Subscribe(cb func(value map[string]Row, rows []Row)) (unsubscribe func()) {
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
	s.subscribers = make(map[int64]func(map[string]Row, []Row))
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

	subs := make([]func(map[string]Row, []Row), 0, len(s.subscribers))
	for _, cb := range s.subscribers {
		subs = append(subs, cb)
	}
	s.mu.RUnlock()

	// Call all subscribers for this notification sequentially
	for _, cb := range subs {
		func(callback func(map[string]Row, []Row)) {
			defer func() {
				if r := recover(); r != nil {
					fmt.Println("panic in subscriber", r)
				}
			}()
			callback(value, rows)
		}(cb)
	}
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
