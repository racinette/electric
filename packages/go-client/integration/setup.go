package integration

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

var (
	globalDBClient     *TestDBClient
	globalSetupMutex   sync.Once
	globalCleanupMutex sync.Once
	globalConfig       *TestConfig
)

// SetupGlobalTestEnvironment sets up the global test schema similar to TypeScript tests
func SetupGlobalTestEnvironment() (*TestDBClient, *TestConfig, error) {
	var setupErr error
	var setupClient *TestDBClient
	var setupConfig *TestConfig

	globalSetupMutex.Do(func() {
		setupConfig = NewTestConfig()

		// Wait for Electric service to be ready
		if err := waitForElectricService(setupConfig.BaseURL); err != nil {
			setupErr = fmt.Errorf("electric service not ready: %w", err)
			return
		}

		// Create admin connection (without schema constraint)
		adminConnStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			setupConfig.DBHost, setupConfig.DBPort, setupConfig.DBUser, setupConfig.DBPassword, setupConfig.DBName)

		adminDB, err := sql.Open("postgres", adminConnStr)
		if err != nil {
			setupErr = fmt.Errorf("failed to open admin database connection: %w", err)
			return
		}

		if err := adminDB.Ping(); err != nil {
			adminDB.Close()
			setupErr = fmt.Errorf("failed to ping admin database: %w", err)
			return
		}

		// Create test schema (similar to TypeScript global setup)
		_, err = adminDB.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", setupConfig.DBSchema))
		if err != nil {
			adminDB.Close()
			setupErr = fmt.Errorf("failed to create test schema: %w", err)
			return
		}
		adminDB.Close()

		// Create test client with schema constraint
		setupClient, err = NewTestDBClient(setupConfig)
		if err != nil {
			setupErr = fmt.Errorf("failed to create test DB client: %w", err)
			return
		}

		globalDBClient = setupClient
		globalConfig = setupConfig

		log.Printf("Global test environment setup complete with schema: %s", setupConfig.DBSchema)
	})

	if setupErr != nil {
		return nil, nil, setupErr
	}

	return globalDBClient, globalConfig, nil
}

// CleanupGlobalTestEnvironment cleans up the global test schema (similar to TypeScript teardown)
func CleanupGlobalTestEnvironment() {
	globalCleanupMutex.Do(func() {
		if globalDBClient == nil || globalConfig == nil {
			return
		}

		// Create admin connection to drop schema
		adminConnStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			globalConfig.DBHost, globalConfig.DBPort, globalConfig.DBUser, globalConfig.DBPassword, globalConfig.DBName)

		adminDB, err := sql.Open("postgres", adminConnStr)
		if err != nil {
			log.Printf("Failed to open admin connection for cleanup: %v", err)
			return
		}
		defer adminDB.Close()

		// Close the test client first
		if err := globalDBClient.Close(); err != nil {
			log.Printf("Failed to close test client: %v", err)
		}

		// Drop the entire test schema (CASCADE to drop all tables)
		_, err = adminDB.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", globalConfig.DBSchema))
		if err != nil {
			log.Printf("Failed to drop test schema: %v", err)
		} else {
			log.Printf("Global test environment cleanup complete")
		}
	})
}

// waitForElectricService waits for Electric service to be ready
func waitForElectricService(baseURL string) error {
	healthURL := fmt.Sprintf("%s/v1/health", baseURL)

	for i := 0; i < 30; i++ {
		resp, err := http.Get(healthURL)
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			return nil
		}
		if resp != nil {
			resp.Body.Close()
		}

		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("electric service not ready after 30 seconds")
}

// TestMain handles global setup and teardown
func TestMain(m *testing.M) {
	// Setup
	_, _, err := SetupGlobalTestEnvironment()
	if err != nil {
		log.Fatalf("Failed to setup global test environment: %v", err)
	}

	// Run tests
	code := m.Run()

	// Cleanup
	CleanupGlobalTestEnvironment()

	os.Exit(code)
}
