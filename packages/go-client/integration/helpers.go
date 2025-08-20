package integration

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"

	goclient "github.com/electric-sql/electric/packages/go-client"
)

// TestConfig holds configuration for integration tests
type TestConfig struct {
	BaseURL    string
	DBHost     string
	DBPort     int
	DBName     string
	DBUser     string
	DBPassword string
	DBSchema   string
}

// NewTestConfig creates test configuration from environment variables
func NewTestConfig() *TestConfig {
	port, _ := strconv.Atoi(getEnvOrDefault("ELECTRIC_DB_PORT", "54321"))
	return &TestConfig{
		BaseURL:    getEnvOrDefault("ELECTRIC_BASE_URL", "http://localhost:3000"),
		DBHost:     getEnvOrDefault("ELECTRIC_DB_HOST", "localhost"),
		DBPort:     port,
		DBName:     getEnvOrDefault("ELECTRIC_DB_NAME", "electric"),
		DBUser:     getEnvOrDefault("ELECTRIC_DB_USER", "postgres"),
		DBPassword: getEnvOrDefault("ELECTRIC_DB_PASSWORD", "password"),
		DBSchema:   getEnvOrDefault("ELECTRIC_DB_SCHEMA", "electric_test"),
	}
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// TestDBClient wraps database operations for testing
type TestDBClient struct {
	db      *sql.DB
	schema  string
	connStr string
	config  *TestConfig
}

// NewTestDBClient creates a new test database client
func NewTestDBClient(config *TestConfig) (*TestDBClient, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable search_path=%s",
		config.DBHost, config.DBPort, config.DBUser, config.DBPassword, config.DBName, config.DBSchema)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &TestDBClient{
		db:      db,
		schema:  config.DBSchema,
		connStr: connStr,
		config:  config,
	}, nil
}

// Close closes the database connection
func (c *TestDBClient) Close() error {
	return c.db.Close()
}

// Query executes a query and returns the result
func (c *TestDBClient) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return c.db.Query(query, args...)
}

// Exec executes a query without returning rows
func (c *TestDBClient) Exec(query string, args ...interface{}) (sql.Result, error) {
	return c.db.Exec(query, args...)
}

// IssueRow represents a test issue record
type IssueRow struct {
	ID       string `json:"id"`
	Title    string `json:"title"`
	Priority *int   `json:"priority,omitempty"`
}

// TestIssuesTable manages a test issues table
type TestIssuesTable struct {
	client    *TestDBClient
	tableName string
	tableURL  string
	tableKey  string
	config    *TestConfig
}

// NewTestIssuesTable creates a new test issues table
func NewTestIssuesTable(client *TestDBClient, config *TestConfig, testID string) (*TestIssuesTable, error) {
	// Generate unique table name for this test - keep it short to avoid PostgreSQL's 63 char limit
	// Use only first 8 chars of UUID and shorter testID
	shortUUID := strings.ReplaceAll(uuid.New().String(), "-", "")[:8]
	// Use unquoted table names to match TypeScript pattern and avoid quote/unquote issues
	tableName := fmt.Sprintf("issues_for_%s_%s", testID, shortUUID)

	// Create table with proper transaction handling similar to TypeScript
	// Use a transaction to ensure atomicity
	tx, err := client.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // Will be ignored if we commit

	// Drop and create in same transaction
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	if _, err := tx.Exec(dropSQL); err != nil {
		return nil, fmt.Errorf("failed to drop existing table: %w", err)
	}

	createSQL := fmt.Sprintf(`CREATE TABLE %s (
		id UUID PRIMARY KEY,
		title TEXT NOT NULL,
		priority INTEGER NOT NULL DEFAULT 10
	)`, tableName)
	if _, err := tx.Exec(createSQL); err != nil {
		return nil, fmt.Errorf("failed to create test table: %w", err)
	}

	commentSQL := fmt.Sprintf("COMMENT ON TABLE %s IS 'Created for integration test %s'", tableName, testID)
	if _, err := tx.Exec(commentSQL); err != nil {
		// Comment failure is not critical, just log it
		// Don't return error for comment failures
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit table creation: %w", err)
	}

	// Table URL format should match TypeScript: schema.table
	tableURL := fmt.Sprintf("%s.%s", client.schema, tableName)
	tableKey := fmt.Sprintf("\"%s\".\"%s\"", client.schema, tableName)

	return &TestIssuesTable{
		client:    client,
		tableName: tableName,
		tableURL:  tableURL,
		tableKey:  tableKey,
		config:    config,
	}, nil
}

// TableName returns the SQL table name
func (t *TestIssuesTable) TableName() string {
	return t.tableName
}

// TableURL returns the table URL for Electric API
func (t *TestIssuesTable) TableURL() string {
	return t.tableURL
}

// TableKey returns the table key for message matching
func (t *TestIssuesTable) TableKey() string {
	return t.tableKey
}

// InsertIssues inserts one or more issues and returns their IDs
func (t *TestIssuesTable) InsertIssues(issues ...IssueRow) ([]string, error) {
	if len(issues) == 0 {
		return nil, nil
	}

	var placeholders []string
	var args []interface{}
	var ids []string

	for i, issue := range issues {
		id := issue.ID
		if id == "" {
			id = uuid.New().String()
		}
		ids = append(ids, id)

		priority := 10
		if issue.Priority != nil {
			priority = *issue.Priority
		}

		baseIndex := i * 3
		placeholders = append(placeholders, fmt.Sprintf("($%d, $%d, $%d)", baseIndex+1, baseIndex+2, baseIndex+3))
		args = append(args, id, issue.Title, priority)
	}

	query := fmt.Sprintf("INSERT INTO %s (id, title, priority) VALUES %s",
		t.tableName, strings.Join(placeholders, ", "))

	if _, err := t.client.Exec(query, args...); err != nil {
		return nil, fmt.Errorf("failed to insert issues: %w", err)
	}

	return ids, nil
}

// UpdateIssue updates an existing issue
func (t *TestIssuesTable) UpdateIssue(id string, updates IssueRow) error {
	var setParts []string
	var args []interface{}
	argIndex := 1

	if updates.Title != "" {
		setParts = append(setParts, fmt.Sprintf("title = $%d", argIndex))
		args = append(args, updates.Title)
		argIndex++
	}

	if updates.Priority != nil {
		setParts = append(setParts, fmt.Sprintf("priority = $%d", argIndex))
		args = append(args, *updates.Priority)
		argIndex++
	}

	if len(setParts) == 0 {
		return nil // No updates to make
	}

	args = append(args, id)
	query := fmt.Sprintf("UPDATE %s SET %s WHERE id = $%d",
		t.tableName, strings.Join(setParts, ", "), argIndex)

	if _, err := t.client.Exec(query, args...); err != nil {
		return fmt.Errorf("failed to update issue: %w", err)
	}
	return nil
}

// DeleteIssue deletes an issue by ID
func (t *TestIssuesTable) DeleteIssue(id string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", t.tableName)
	if _, err := t.client.Exec(query, id); err != nil {
		return fmt.Errorf("failed to delete issue: %w", err)
	}
	return nil
}

// Cleanup drops the test table safely
func (t *TestIssuesTable) Cleanup() error {
	// Don't cleanup immediately - let the global schema cleanup handle it
	// This prevents race conditions during concurrent tests
	// The table will be cleaned up when the entire test schema is dropped
	return nil
}

// TableExists checks if the table exists in the database
func (t *TestIssuesTable) TableExists() bool {
	query := fmt.Sprintf("SELECT 1 FROM %s LIMIT 1", t.tableName)
	_, err := t.client.Query(query)
	return err == nil
}

// ClearShape clears a shape via HTTP DELETE
func ClearShape(baseURL, table string, handle string) error {
	u, err := url.Parse(fmt.Sprintf("%s/v1/shape", baseURL))
	if err != nil {
		return fmt.Errorf("invalid base URL: %w", err)
	}

	q := u.Query()
	q.Set("table", table)
	if handle != "" {
		q.Set("handle", handle)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

// WaitForTransaction waits for a specific number of changes to be processed
func WaitForTransaction(config *TestConfig, table string, numChangesExpected int, streamOptions *goclient.Options) (*goclient.Options, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	opts := goclient.Options{
		URL:       fmt.Sprintf("%s/v1/shape", config.BaseURL),
		Params:    goclient.NewParams(table),
		Subscribe: true,
		Offset:    "-1", // Explicitly set default offset
	}

	// Copy over any existing stream options
	if streamOptions != nil {
		if streamOptions.Offset != "" {
			opts.Offset = streamOptions.Offset
		}
		if streamOptions.Handle != "" {
			opts.Handle = streamOptions.Handle
		}
		if streamOptions.Params != nil {
			opts.Params = streamOptions.Params.DeepCopy()
		}
	}

	stream, err := goclient.NewShapeStream(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create shape stream: %w", err)
	}
	defer stream.Close()

	numChangesSeen := 0
	done := make(chan *goclient.Options, 1)
	errCh := make(chan error, 1)

	unsubscribe := stream.Subscribe(func(messages []goclient.Message) {
		for _, msg := range messages {
			if goclient.IsChangeMessage(msg) {
				numChangesSeen++
			}
			if numChangesSeen >= numChangesExpected && goclient.IsUpToDateMessage(msg) {
				done <- &goclient.Options{
					Offset: stream.LastOffset(),
					Handle: stream.ShapeHandle(),
				}
				return
			}
		}
	})
	defer unsubscribe()

	select {
	case result := <-done:
		return result, nil
	case err := <-errCh:
		return nil, err
	case <-ctx.Done():
		return nil, fmt.Errorf("timeout waiting for transaction")
	}
}

// ForEachMessage helper for processing messages with early termination
func ForEachMessage(stream *goclient.ShapeStream, ctx context.Context, handler func(resolve func(), msg goclient.Message, nthDataMessage int) error) error {
	done := make(chan struct{})
	errCh := make(chan error, 1)
	messageIdx := 0

	unsubscribe := stream.Subscribe(func(messages []goclient.Message) {
		for _, message := range messages {
			err := handler(func() {
				close(done)
			}, message, messageIdx)
			if err != nil {
				errCh <- err
				return
			}
			if goclient.IsChangeMessage(message) {
				messageIdx++
			}
		}
	})
	defer unsubscribe()

	select {
	case <-done:
		return nil
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
