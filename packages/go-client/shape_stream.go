package goclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Params and Headers can accept either concrete values or provider functions.
type ValueFunc func(ctx context.Context) (string, error)

// ShapeParams represents the parameters for creating a shape with explicit
// typing for PostgreSQL-specific parameters and a map for additional parameters.
type ShapeParams struct {
	// PostgreSQL-specific parameters with explicit typing
	table   string
	where   *string
	columns []string
	replica *Replica

	whereParams []string

	// Additional custom parameters - embedded map for any other parameters
	// that should be passed to the request
	additional map[string]string
}

func (p *ShapeParams) DeepCopy() *ShapeParams {
	var additional map[string]string
	if p.additional != nil {
		additional = make(map[string]string, len(p.additional))
		for k, v := range p.additional {
			additional[k] = v
		}
	}
	return &ShapeParams{
		table:       p.table,
		where:       p.where,
		columns:     p.columns,
		replica:     p.replica,
		whereParams: p.whereParams,
		additional:  additional,
	}
}

// NewParams creates a new ShapeParams instance with initialized Additional map
func NewParams(table string) *ShapeParams {
	return &ShapeParams{
		table:      table,
		additional: make(map[string]string),
	}
}

// WithWhere sets the where clause parameter
func (p *ShapeParams) WithWhere(where string, params ...string) *ShapeParams {
	p.where = &where
	if len(params) > 0 {
		p.whereParams = params
	}
	return p
}

// WithColumns sets the columns parameter
func (p *ShapeParams) WithColumns(columns ...string) *ShapeParams {
	p.columns = columns
	return p
}

// WithReplica sets the replica parameter
func (p *ShapeParams) WithReplica(replica Replica) *ShapeParams {
	p.replica = &replica
	return p
}

// WithAdditional adds an additional custom parameter
func (p *ShapeParams) WithAdditional(key string, value string) *ShapeParams {
	if p.additional == nil {
		p.additional = make(map[string]string)
	}
	p.additional[key] = value
	return p
}

// StringPtr is a helper function to create a string pointer
func StringPtr(s string) *string {
	return &s
}

type Headers map[string]string

type Replica string

const (
	ReplicaDefault Replica = "default"
	ReplicaFull    Replica = "full"
)

type ErrorHandler func(err error) (retryOpts *RetryOptions)

type RetryOptions struct {
	Params  *ShapeParams
	Headers Headers
}

type Options struct {
	URL     string
	Offset  Offset
	Handle  string
	Headers Headers

	Params *ShapeParams

	Subscribe           bool
	ExperimentalLiveSse bool // Experimental support for Server-Sent Events (SSE) for live updates
	Backoff             BackoffOptions
	HTTPClient          *http.Client
	OnError             ErrorHandler
	Ctx                 context.Context
}

type Subscriber func(messages []Message)

// abortController mimics AbortController behavior from JavaScript
type abortController struct {
	ctx    context.Context
	cancel context.CancelFunc
	reason string
}

func newAbortController() *abortController {
	ctx, cancel := context.WithCancel(context.Background())
	return &abortController{
		ctx:    ctx,
		cancel: cancel,
	}
}

func (ac *abortController) abort(reason string) {
	ac.reason = reason
	ac.cancel()
}

func (ac *abortController) signal() context.Context {
	return ac.ctx
}

type ShapeStream struct {
	opts Options

	// Atomic fields - must be first for proper alignment
	subscriptionCounter int64

	// Protected by mutex
	mu              sync.RWMutex
	started         bool
	state           string // active | pause-requested | paused
	lastOffset      Offset
	liveCacheBuster string
	lastSyncedAt    *time.Time
	isUpToDate      bool
	connected       bool
	shapeHandle     string
	schema          Schema
	err             error
	isRefreshing    bool

	subscribers map[int64]Subscriber
	httpClient  *http.Client
	cancel      context.CancelFunc

	// Channels for coordination
	requestAbortController *abortController
	messageChain           chan struct{}

	// Message serialization channel to prevent race conditions
	messageQueue       chan []Message
	messageQueueCancel context.CancelFunc
}

func NewShapeStream(opts Options) (*ShapeStream, error) {
	// Validate required options
	if opts.URL == "" {
		return nil, MissingShapeURLError{}
	}

	// Validate offset and handle relationship
	if opts.Offset != "" && opts.Offset != "-1" && opts.Handle == "" {
		return nil, MissingShapeHandleError{}
	}

	// Set defaults
	if opts.HTTPClient == nil {
		opts.HTTPClient = http.DefaultClient
	}

	// Default subscribe to true (since zero value is false, we need to explicitly set it)
	// In Go, we can't distinguish between explicitly set false and zero value false
	// So we always set it to true as the default, matching TypeScript behavior
	if !opts.Subscribe {
		opts.Subscribe = true
	}

	// Set default offset
	if opts.Offset == "" {
		opts.Offset = "-1"
	}

	// Validate params against reserved names
	if err := ValidateParams(opts.Params); err != nil {
		return nil, err
	}

	// Create context for message queue
	messageQueueCtx, messageQueueCancel := context.WithCancel(context.Background())

	ss := &ShapeStream{
		opts:               opts,
		state:              "active",
		lastOffset:         opts.Offset,
		shapeHandle:        opts.Handle,
		subscribers:        make(map[int64]Subscriber),
		httpClient:         opts.HTTPClient,
		messageChain:       make(chan struct{}, 1),
		messageQueue:       make(chan []Message, 100), // Buffered channel for message batches
		messageQueueCancel: messageQueueCancel,
	}

	// Start message serialization worker
	go ss.messageWorker(messageQueueCtx)

	return ss, nil
}

func (s *ShapeStream) Subscribe(cb Subscriber) (unsubscribe func()) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Generate thread-safe unique ID
	id := atomic.AddInt64(&s.subscriptionCounter, 1)
	s.subscribers[id] = cb

	shouldStart := !s.started
	if shouldStart {
		s.started = true
		go s.start()
	}

	return func() {
		s.mu.Lock()
		delete(s.subscribers, id)
		s.mu.Unlock()
	}
}

func (s *ShapeStream) start() {
	base := s.opts.Ctx
	if base == nil {
		base = context.Background()
	}
	ctx, cancel := context.WithCancel(base)

	s.mu.Lock()
	s.cancel = cancel
	s.mu.Unlock()

	defer func() {
		// Always disconnect when start() exits
		s.mu.Lock()
		s.connected = false
		s.mu.Unlock()
	}()

	s.requestShape(ctx)
}

func (s *ShapeStream) requestShape(ctx context.Context) {
	for {
		s.mu.RLock()
		state := s.state
		isUpToDate := s.isUpToDate
		subscribe := s.opts.Subscribe
		s.mu.RUnlock()

		// Handle pause state
		if state == "pause-requested" {
			s.mu.Lock()
			s.state = "paused"
			s.mu.Unlock()
			return
		}

		// Check if we should stop
		if !subscribe && isUpToDate {
			return
		}

		// Check context cancellation
		if ctx.Err() != nil {
			return
		}

		// Try to make the request
		if err := s.fetchShape(ctx); err != nil {
			if err == context.Canceled {
				return
			}
			s.handleError(err)
			return
		}
	}
}

func (s *ShapeStream) fetchShape(ctx context.Context) error {
	// Create abort controller for this request
	s.requestAbortController = newAbortController()
	defer func() { s.requestAbortController = nil }()

	// Build URL and headers
	fetchURL, headers, err := s.buildURL()
	if err != nil {
		return err
	}

	// Create request
	req, err := http.NewRequestWithContext(s.requestAbortController.signal(), http.MethodGet, fetchURL, nil)
	if err != nil {
		return err
	}

	// Set headers
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	// Make request with backoff
	backoffOpts := s.opts.Backoff
	backoffOpts.OnFailedAttempt = func() {
		s.mu.Lock()
		s.connected = false
		s.mu.Unlock()
	}

	resp, err := FetchWithBackoff(ctx, s.httpClient, req, backoffOpts)
	if err != nil {
		// Check for abort with specific reason
		if s.requestAbortController != nil && s.requestAbortController.ctx.Err() != nil {
			reason := s.requestAbortController.reason
			if reason == ForceDisconnectAndRefresh {
				return s.fetchShape(ctx) // Retry
			}
			if reason == PauseStream {
				s.mu.Lock()
				s.state = "paused"
				s.mu.Unlock()
				return nil
			}
		}
		return err
	}

	// Handle non-success status codes
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Read body first before it gets consumed by buildFetchError
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		fetchErr := s.buildFetchErrorWithBody(resp, fetchURL, body)

		// Handle 409 (shape rotation)
		if resp.StatusCode == http.StatusConflict {
			newHandle := resp.Header.Get(ShapeHandleHeader)

			var msgs []Message
			if len(body) > 0 {
				// Use empty schema for control messages since schema might be nil
				s.mu.RLock()
				schema := s.schema
				s.mu.RUnlock()
				if schema == nil {
					schema = Schema{}
				}
				if parsedMsgs, err := ParseMessages(body, schema); err == nil {
					msgs = parsedMsgs
				}
			}

			s.resetWithControlMessages(newHandle, msgs)
			return s.fetchShape(ctx) // Retry with new handle
		}

		return fetchErr
	}

	// Verify required headers
	if err := s.checkRequiredHeaders(fetchURL, resp.Header); err != nil {
		resp.Body.Close()
		return err
	}

	// Mark as connected and process initial response
	s.mu.Lock()
	s.connected = true
	s.mu.Unlock()

	s.onInitialResponse(resp)

	// Read and parse response body
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}

	if len(body) == 0 {
		body = []byte("[]")
	}

	msgs, err := ParseMessages(body, s.schema)
	if err != nil {
		return err
	}

	s.onMessages(msgs)
	return nil
}

func (s *ShapeStream) onInitialResponse(resp *http.Response) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if h := resp.Header.Get(ShapeHandleHeader); h != "" {
		s.shapeHandle = h
	}
	if o := resp.Header.Get(ChunkLastOffsetHeader); o != "" {
		s.lastOffset = Offset(o)
	}
	if c := resp.Header.Get(LiveCacheBusterHeader); c != "" {
		s.liveCacheBuster = c
	}

	// Initialize schema if not set
	if s.schema == nil {
		if sc := resp.Header.Get(ShapeSchemaHeader); sc != "" {
			var schema Schema
			if err := json.Unmarshal([]byte(sc), &schema); err == nil {
				s.schema = schema
			}
		} else {
			s.schema = Schema{}
		}
	}

	// Handle 204 No Content (deprecated but kept for backwards compatibility)
	if resp.StatusCode == http.StatusNoContent {
		now := time.Now()
		s.lastSyncedAt = &now
	}
}

func (s *ShapeStream) onMessages(batch []Message) {
	if len(batch) == 0 {
		return
	}

	// Check if last message is up-to-date
	last := batch[len(batch)-1]
	if IsUpToDateMessage(last) {
		s.mu.Lock()
		now := time.Now()
		s.lastSyncedAt = &now
		s.isUpToDate = true
		s.mu.Unlock()
	}

	s.publish(batch)
}

func (s *ShapeStream) publish(msgs []Message) {
	if len(msgs) == 0 {
		return
	}

	// This ensures message batches are processed sequentially, preventing race conditions.
	// By blocking when the channel is full, we apply backpressure to the fetch process.
	s.messageQueue <- msgs
}

func (s *ShapeStream) buildURL() (string, map[string]string, error) {
	// Resolve headers
	headers := maps.Clone(s.opts.Headers)

	// Validate params against reserved names
	if err := ValidateParamKeys(s.opts.Params.additional); err != nil {
		return "", nil, err
	}

	u, err := url.Parse(s.opts.URL)
	if err != nil {
		return "", nil, err
	}

	q := u.Query()

	for k, v := range s.opts.Params.additional {
		q.Set(k, v)
	}

	// Set PostgreSQL-specific parameters
	q.Set(TableQueryParam, s.opts.Params.table)
	if s.opts.Params.where != nil {
		q.Set(WhereQueryParam, *s.opts.Params.where)
	}
	if len(s.opts.Params.columns) > 0 {
		q.Set(ColumnsQueryParam, strings.Join(s.opts.Params.columns, ","))
	}
	if s.opts.Params.replica != nil {
		q.Set(ReplicaParam, string(*s.opts.Params.replica))
	}
	if s.opts.Params.whereParams != nil {
		for i, v := range s.opts.Params.whereParams {
			// params in postgres are 1-indexed
			q.Set(fmt.Sprintf("params[%d]", i+1), v)
		}
	}

	// Set Electric's internal parameters
	s.mu.RLock()
	lastOffset := s.lastOffset
	isUpToDate := s.isUpToDate
	liveCacheBuster := s.liveCacheBuster
	shapeHandle := s.shapeHandle
	isRefreshing := s.isRefreshing
	s.mu.RUnlock()

	q.Set(OffsetQueryParam, string(lastOffset))

	if isUpToDate && !isRefreshing {
		q.Set(LiveQueryParam, "true")
		q.Set(LiveCacheBusterParam, liveCacheBuster)
	}

	if shapeHandle != "" {
		q.Set(ShapeHandleQueryParam, shapeHandle)
	}

	// Sort query parameters for stable URLs
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	enc := url.Values{}
	for _, k := range keys {
		enc[k] = q[k]
	}
	u.RawQuery = enc.Encode()

	return u.String(), headers, nil
}

func (s *ShapeStream) buildFetchErrorWithBody(resp *http.Response, url string, body []byte) error {
	headers := map[string]string{}
	for k, v := range resp.Header {
		if len(v) > 0 {
			headers[k] = v[0]
		}
	}

	var text string
	var js map[string]any

	ct := resp.Header.Get("content-type")
	if strings.Contains(ct, "application/json") {
		if err := json.Unmarshal(body, &js); err != nil {
			text = string(body)
		}
	} else {
		text = string(body)
	}

	return &FetchError{
		Status:  resp.StatusCode,
		Text:    text,
		JSON:    js,
		Headers: headers,
		URL:     url,
	}
}

func (s *ShapeStream) checkRequiredHeaders(requestURL string, headers http.Header) error {
	required := []string{ChunkLastOffsetHeader, ShapeHandleHeader}

	reqURL, _ := url.Parse(requestURL)
	live := reqURL.Query().Get(LiveQueryParam) == "true"

	if live {
		required = append(required, LiveCacheBusterHeader)
	} else {
		required = append(required, ShapeSchemaHeader)
	}

	var missing []string
	for _, h := range required {
		if headers.Get(h) == "" {
			missing = append(missing, h)
		}
	}

	if len(missing) > 0 {
		return MissingHeadersError{
			URL:            requestURL,
			MissingHeaders: missing,
		}
	}

	return nil
}

func (s *ShapeStream) handleError(err error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()

	if s.opts.OnError != nil {
		if ro := s.opts.OnError(err); ro != nil {
			// Apply retry options and restart
			if ro.Params != nil {
				s.opts.Params = ro.Params
			}
			if ro.Headers != nil {
				s.opts.Headers = ro.Headers
			}

			s.reset("")

			// Restart the stream
			go s.start()
			return
		}
	}

	// If no retry, just store the error - subscribers handle their own error handling
}

// Helpers - all thread-safe
func (s *ShapeStream) IsConnected() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.connected
}

func (s *ShapeStream) IsLoading() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return !s.isUpToDate
}

func (s *ShapeStream) IsUpToDate() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.isUpToDate
}

func (s *ShapeStream) LastSyncedAt() *time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastSyncedAt
}

func (s *ShapeStream) LastSynced() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.lastSyncedAt == nil {
		return time.Duration(1<<63 - 1) // Max duration
	}
	return time.Since(*s.lastSyncedAt)
}

func (s *ShapeStream) LastOffset() Offset {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastOffset
}

func (s *ShapeStream) ShapeHandle() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.shapeHandle
}

func (s *ShapeStream) Error() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.err
}

func (s *ShapeStream) HasStarted() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.started
}

// NumSubscribers returns the number of active subscribers
func (s *ShapeStream) NumSubscribers() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.subscribers)
}

// Schema returns the current schema if available
func (s *ShapeStream) Schema() Schema {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.schema
}

func (s *ShapeStream) reset(handle string) {
	s.resetWithControlMessages(handle, nil)
}

// resetWithControlMessages stops the stream, delivers optional control messages,
// and then resets the stream to a clean state. This is used for shape rotations (409)
// and error recovery.
func (s *ShapeStream) resetWithControlMessages(handle string, msgs []Message) {
	s.mu.Lock()
	// Stop current message processing
	if s.messageQueueCancel != nil {
		s.messageQueueCancel()
	}
	s.mu.Unlock()

	// Deliver critical messages synchronously after stopping the queue
	if len(msgs) > 0 {
		s.callSubscribersSync(msgs)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	// Reset state and start new message processor
	messageQueueCtx, messageQueueCancel := context.WithCancel(context.Background())
	s.messageQueue = make(chan []Message, 100)
	s.messageQueueCancel = messageQueueCancel
	go s.messageWorker(messageQueueCtx)

	s.lastOffset = "-1"
	s.liveCacheBuster = ""
	s.shapeHandle = handle
	s.isUpToDate = false
	s.connected = false
	s.schema = nil
}

// ForceDisconnectAndRefresh aborts live long poll and issues a refresh request.
func (s *ShapeStream) ForceDisconnectAndRefresh() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.isUpToDate {
		return nil
	}

	s.isRefreshing = true
	defer func() { s.isRefreshing = false }()

	// Abort current request if running
	if s.requestAbortController != nil {
		s.requestAbortController.abort(ForceDisconnectAndRefresh)
	}

	return nil
}

// UnsubscribeAll removes all subscribers
func (s *ShapeStream) UnsubscribeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscribers = make(map[int64]Subscriber)
}

// Pause temporarily stops the stream
func (s *ShapeStream) Pause() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started && s.state == "active" {
		s.state = "pause-requested"
		if s.requestAbortController != nil {
			s.requestAbortController.abort(PauseStream)
		}
	}
}

// Resume resumes the stream if it was paused
func (s *ShapeStream) Resume() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started && (s.state == "paused" || s.state == "pause-requested") {
		s.state = "active"
		go s.start()
	}
}

// IsPaused returns true if the stream is currently paused or pause is requested
func (s *ShapeStream) IsPaused() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state == "paused" || s.state == "pause-requested"
}

// messageWorker processes messages sequentially to prevent race conditions
func (s *ShapeStream) messageWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msgs := <-s.messageQueue:
			// Process this batch of messages sequentially
			s.callSubscribersSync(msgs)
		}
	}
}

// callSubscribersSync calls all subscribers synchronously for a batch of messages
func (s *ShapeStream) callSubscribersSync(msgs []Message) {
	s.mu.RLock()
	subs := make([]Subscriber, 0, len(s.subscribers))
	for _, cb := range s.subscribers {
		subs = append(subs, cb)
	}
	s.mu.RUnlock()

	// Call all subscribers for this batch of messages sequentially
	for _, cb := range subs {
		func(callback Subscriber) {
			defer func() {
				if r := recover(); r != nil {
					// Log panic but don't crash the stream
					fmt.Printf("Subscriber panic: %v\n", r)
				}
			}()
			callback(msgs)
		}(cb)
	}
}

// Close stops the stream and prevents restarts.
func (s *ShapeStream) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cancel != nil {
		s.cancel()
	}

	if s.requestAbortController != nil {
		s.requestAbortController.abort("close")
	}

	// Stop message worker
	if s.messageQueueCancel != nil {
		s.messageQueueCancel()
	}

	s.started = false
	s.state = "paused"
	s.subscribers = make(map[int64]Subscriber)
}
