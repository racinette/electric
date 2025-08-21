package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	goclient "github.com/electric-sql/electric/packages/go-client"
)

// TestMultitypeTable manages a test table with various PostgreSQL data types
type TestMultitypeTable struct {
	client    *TestDBClient
	tableName string
	tableURL  string
	config    *TestConfig
}

// NewTestMultitypeTable creates a test table with various PostgreSQL data types
func NewTestMultitypeTable(client *TestDBClient, config *TestConfig, testID string) (*TestMultitypeTable, error) {
	tableName := fmt.Sprintf("\"multitype_table_for_%s_%s\"", testID, uuid.New().String()[:8])

	// Create table with various data types similar to TypeScript tests
	createSQL := fmt.Sprintf(`
		DROP TABLE IF EXISTS %s;
		DROP TYPE IF EXISTS mood CASCADE;
		DROP TYPE IF EXISTS complex CASCADE;
		DROP DOMAIN IF EXISTS posint CASCADE;
		
		CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
		CREATE TYPE complex AS (r double precision, i double precision);
		CREATE DOMAIN posint AS integer CHECK (VALUE > 0);
		
		CREATE TABLE %s (
			txt VARCHAR,
			i2 INT2 PRIMARY KEY,
			i4 INT4,
			i8 INT8,
			f8 FLOAT8,
			b  BOOLEAN,
			num NUMERIC,
			json JSON,
			jsonb JSONB,
			ints INT8[],
			ints2 INT8[][],
			int4s INT4[],
			doubles FLOAT8[],
			bools BOOLEAN[],
			moods mood[],
			moods2 mood[][],
			complexes complex[],
			posints posint[],
			jsons JSONB[],
			txts TEXT[],
			value JSON
		);
		COMMENT ON TABLE %s IS 'Created for data type test %s';
	`, tableName, tableName, tableName, testID)

	if _, err := client.Exec(createSQL); err != nil {
		return nil, fmt.Errorf("failed to create multitype test table: %w", err)
	}

	tableURL := fmt.Sprintf("%s.%s", client.schema, tableName)

	return &TestMultitypeTable{
		client:    client,
		tableName: tableName,
		tableURL:  tableURL,
		config:    config,
	}, nil
}

// TableURL returns the table URL for Electric API
func (t *TestMultitypeTable) TableURL() string {
	return t.tableURL
}

// InsertTestData inserts complex test data similar to TypeScript tests
func (t *TestMultitypeTable) InsertTestData() error {
	// Insert data similar to TypeScript integration test
	insertSQL := fmt.Sprintf(`
		INSERT INTO %s (
			txt, i2, i4, i8, f8, b, json, jsonb, ints, ints2, int4s, 
			bools, moods, moods2, complexes, posints, jsons, txts, value, doubles, num
		) VALUES (
			'test',
			1,
			2147483647,
			9223372036854775807,
			4.5,
			TRUE,
			'{"foo": "bar"}',
			'{"foo": "bar"}',
			'{1,2,3}',
			$1,
			$2,
			$3,
			$4,
			$5,
			$6,
			$7,
			$8,
			$9,
			$10,
			$11,
			$12
		)
	`, t.tableName)

	args := []interface{}{
		"{{1,2,3},{4,5,6}}",             // ints2
		"{1,2,3}",                       // int4s
		"{true,false,true}",             // bools
		"{sad,ok,happy}",                // moods
		"{{sad,ok},{ok,happy}}",         // moods2
		"{\"(1.1,2.2)\",\"(3.3,4.4)\"}", // complexes
		"{5,9,2}",                       // posints
		"{\"{\\\"foo\\\": \\\"bar\\\"}\", \"{\\\"bar\\\": \\\"baz\\\"}\"}", // jsons
		"{foo,bar,baz}",                          // txts
		"{\"a\": 5, \"b\": [{\"c\": \"foo\"}]}",  // value
		"{\"Infinity\", \"-Infinity\", \"NaN\"}", // doubles
		"123.456",                                // num
	}

	if _, err := t.client.Exec(insertSQL, args...); err != nil {
		return fmt.Errorf("failed to insert test data: %w", err)
	}

	return nil
}

// UpdateTestData updates the test data
func (t *TestMultitypeTable) UpdateTestData() error {
	updateSQL := fmt.Sprintf(`
		UPDATE %s SET
			txt = 'changed',
			i4 = 20,
			i8 = 30,
			f8 = 40.5,
			b = FALSE,
			json = '{"bar": "foo"}',
			jsonb = '{"bar": "foo"}',
			ints = '{4,5,6}',
			ints2 = '{{4,5,6},{7,8,9}}',
			int4s = '{4,5,6}',
			bools = $1,
			moods = '{sad,happy}',
			moods2 = '{{sad,happy},{happy,ok}}',
			complexes = $2,
			posints = '{6,10,3}',
			jsons = $3,
			txts = $4,
			value = $5,
			doubles = $6,
			num = $7
		WHERE i2 = 1
	`, t.tableName)

	args := []interface{}{
		"{false,true,false}",                     // bools
		"{\"(2.2,3.3)\",\"(4.4,5.5)\"}",          // complexes
		"{\"{}\"}",                               // jsons
		"{new,values}",                           // txts
		"{\"a\": 6}",                             // value
		"{\"Infinity\", \"NaN\", \"-Infinity\"}", // doubles
		"789.012",                                // num
	}

	if _, err := t.client.Exec(updateSQL, args...); err != nil {
		return fmt.Errorf("failed to update test data: %w", err)
	}

	return nil
}

// MultitypeRow represents a row in the multitype table
type MultitypeRow struct {
	Txt       string
	I2        int
	I4        *int
	I8        *int64
	F8        *float64
	B         *bool
	Num       *string
	JSON      *string
	JSONB     *string
	Ints      *string
	Ints2     *string
	Int4s     *string
	Doubles   *string
	Bools     *string
	Moods     *string
	Moods2    *string
	Complexes *string
	Posints   *string
	Jsons     *string
	Txts      *string
	Value     *string
}

// InsertTestRows inserts multiple rows of test data
func (t *TestMultitypeTable) InsertTestRows(rows []MultitypeRow) error {
	if len(rows) == 0 {
		return nil
	}

	valueStrings := make([]string, 0, len(rows))
	valueArgs := make([]interface{}, 0, len(rows)*21)

	for i, row := range rows {
		base := i * 21
		placeholders := fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9, base+10,
			base+11, base+12, base+13, base+14, base+15, base+16, base+17, base+18, base+19, base+20, base+21)
		valueStrings = append(valueStrings, placeholders)

		valueArgs = append(valueArgs, row.Txt, row.I2, row.I4, row.I8, row.F8, row.B, row.Num, row.JSON, row.JSONB,
			row.Ints, row.Ints2, row.Int4s, row.Doubles, row.Bools, row.Moods, row.Moods2,
			row.Complexes, row.Posints, row.Jsons, row.Txts, row.Value)
	}

	stmt := fmt.Sprintf(`INSERT INTO %s (
		txt, i2, i4, i8, f8, b, num, json, jsonb, 
		ints, ints2, int4s, doubles, bools, moods, moods2, 
		complexes, posints, jsons, txts, value
	) VALUES %s`, t.tableName, strings.Join(valueStrings, ","))

	_, err := t.client.Exec(stmt, valueArgs...)
	return err
}

// UpdateTestRow updates a single row
func (t *TestMultitypeTable) UpdateTestRow(i2 int, updates MultitypeRow) error {
	var setParts []string
	var args []interface{}
	argIdx := 1

	addUpdate := func(field string, value interface{}) {
		if value != nil {
			setParts = append(setParts, fmt.Sprintf("%s = $%d", field, argIdx))
			args = append(args, value)
			argIdx++
		}
	}

	addUpdate("txt", &updates.Txt)
	addUpdate("i4", updates.I4)
	addUpdate("i8", updates.I8)
	addUpdate("f8", updates.F8)
	addUpdate("b", updates.B)
	addUpdate("num", updates.Num)

	if len(setParts) == 0 {
		return nil // No updates
	}

	args = append(args, i2)
	query := fmt.Sprintf("UPDATE %s SET %s WHERE i2 = $%d", t.tableName, strings.Join(setParts, ", "), argIdx)

	_, err := t.client.Exec(query, args...)
	return err
}

// DeleteTestRow deletes a single row
func (t *TestMultitypeTable) DeleteTestRow(i2 int) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE i2 = $1", t.tableName)
	_, err := t.client.Exec(query, i2)
	return err
}

// Cleanup drops the test table and types
func (t *TestMultitypeTable) Cleanup() error {
	cleanupSQL := fmt.Sprintf(`
		DROP TABLE IF EXISTS %s;
		DROP TYPE IF EXISTS mood CASCADE;
		DROP TYPE IF EXISTS complex CASCADE;
		DROP DOMAIN IF EXISTS posint CASCADE;
	`, t.tableName)

	_, err := t.client.Exec(cleanupSQL)
	return err
}

func TestDataTypeParsing(t *testing.T) {
	config := NewTestConfig()
	dbClient, err := NewTestDBClient(config)
	require.NoError(t, err)
	defer dbClient.Close()

	// Test both long polling modes
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
			testDataTypeParsing(t, config, dbClient, tc.experimentalLiveSSE)
		})
	}
}

func testDataTypeParsing(t *testing.T, config *TestConfig, dbClient *TestDBClient, experimentalLiveSSE bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create test table
	table, err := NewTestMultitypeTable(dbClient, config, "data_types")
	require.NoError(t, err)
	defer table.Cleanup()

	// Insert test data
	err = table.InsertTestData()
	require.NoError(t, err)

	// Wait for transaction to be processed
	_, err = WaitForTransaction(config, table.TableURL(), 1, nil)
	require.NoError(t, err)

	// Create shape stream
	stream, err := goclient.NewShapeStream(goclient.Options{
		URL:                 config.BaseURL + "/v1/shape",
		Params:              goclient.NewParams(table.TableURL()),
		Subscribe:           true, // We'll test updates too
		ExperimentalLiveSse: experimentalLiveSSE,
	})
	require.NoError(t, err)
	defer stream.Close()

	shape := goclient.NewShape(stream)
	defer shape.Close()

	// Wait for initial sync
	assert.Eventually(t, func() bool {
		return shape.IsUpToDate() && len(shape.CurrentRows()) == 1
	}, 15*time.Second, 100*time.Millisecond)

	// Verify initial data types
	rows := shape.CurrentRows()
	require.Len(t, rows, 1)
	row := rows[0]

	// Test basic types
	assert.Equal(t, "test", row["txt"])
	assert.Equal(t, 1, row["i2"])          // i2 should be parsed as int based on schema
	assert.Equal(t, 2147483647, row["i4"]) // i4 should be parsed as int based on schema
	// Note: Large int64 values may be returned as strings to preserve precision
	assert.Equal(t, 4.5, row["f8"])
	assert.Equal(t, true, row["b"])

	// Test numeric type
	// Note: NUMERIC values are often returned as strings to preserve precision
	assert.Equal(t, "123.456", row["num"])

	// Test JSON types
	jsonObj, ok := row["json"].(map[string]interface{})
	require.True(t, ok, "JSON should be parsed as object")
	assert.Equal(t, "bar", jsonObj["foo"])

	jsonbObj, ok := row["jsonb"].(map[string]interface{})
	require.True(t, ok, "JSONB should be parsed as object")
	assert.Equal(t, "bar", jsonbObj["foo"])

	// Test arrays - these will be parsed as JSON arrays
	intsArray, ok := row["ints"].([]interface{})
	require.True(t, ok, "ints should be parsed as array")
	require.Len(t, intsArray, 3)

	int4sArray, ok := row["int4s"].([]interface{})
	require.True(t, ok, "int4s should be parsed as array")
	require.Len(t, int4sArray, 3)

	boolsArray, ok := row["bools"].([]interface{})
	require.True(t, ok, "bools should be parsed as array")
	require.Len(t, boolsArray, 3)

	// Test complex value
	valueObj, ok := row["value"].(map[string]interface{})
	require.True(t, ok, "value should be parsed as object")
	assert.Equal(t, float64(5), valueObj["a"])

	// Track updates for testing data modifications
	updateCount := 0
	var finalRow goclient.Row
	updateDone := make(chan bool)

	unsubscribe := shape.Subscribe(func(value map[string]goclient.Row, rows []goclient.Row, diff goclient.Diff) {
		updateCount++
		if len(rows) > 0 {
			finalRow = rows[0]
		}
		if updateCount >= 2 { // Initial + update
			updateDone <- true
		}
	})
	defer unsubscribe()

	// Update the data
	err = table.UpdateTestData()
	require.NoError(t, err)

	// Wait for update to be received
	select {
	case <-updateDone:
		// Success
	case <-ctx.Done():
		t.Fatal("Timeout waiting for data type update")
	}

	// Verify updated data
	assert.Equal(t, "changed", finalRow["txt"])
	assert.Equal(t, float64(20), finalRow["i4"])
	assert.Equal(t, float64(30), finalRow["i8"])
	assert.Equal(t, 40.5, finalRow["f8"])
	assert.Equal(t, false, finalRow["b"])

	// Test updated numeric type
	assert.Equal(t, "789.012", finalRow["num"])

	// Test updated JSON
	updatedJsonObj, ok := finalRow["json"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "foo", updatedJsonObj["bar"])

	updatedJsonbObj, ok := finalRow["jsonb"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "foo", updatedJsonbObj["bar"])

	// Test updated arrays
	updatedInts, ok := finalRow["ints"].([]interface{})
	require.True(t, ok)
	require.Len(t, updatedInts, 3)

	updatedBools, ok := finalRow["bools"].([]interface{})
	require.True(t, ok)
	require.Len(t, updatedBools, 3)
	assert.Equal(t, false, updatedBools[0])
	assert.Equal(t, true, updatedBools[1])
	assert.Equal(t, false, updatedBools[2])

	// Test updated complex value
	updatedValueObj, ok := finalRow["value"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, float64(6), updatedValueObj["a"])
}

func TestColumnSelection(t *testing.T) {
	config := NewTestConfig()
	dbClient, err := NewTestDBClient(config)
	require.NoError(t, err)
	defer dbClient.Close()

	// Create test table
	table, err := NewTestMultitypeTable(dbClient, config, "column_selection")
	require.NoError(t, err)
	defer table.Cleanup()

	// Insert test data
	err = table.InsertTestData()
	require.NoError(t, err)

	// Wait for transaction
	_, err = WaitForTransaction(config, table.TableURL(), 1, nil)
	require.NoError(t, err)

	// Create shape stream with column selection
	stream, err := goclient.NewShapeStream(goclient.Options{
		URL: config.BaseURL + "/v1/shape",
		Params: goclient.NewParams(table.TableURL()).
			WithColumns("txt", "i2", "i4"), // Only select specific columns
		Subscribe: true,
	})
	require.NoError(t, err)
	defer stream.Close()

	shape := goclient.NewShape(stream)
	defer shape.Close()

	// Wait for initial sync
	assert.Eventually(t, func() bool {
		return shape.IsUpToDate() && len(shape.CurrentRows()) == 1
	}, 10*time.Second, 100*time.Millisecond)

	// Verify only selected columns are present
	rows := shape.CurrentRows()
	require.Len(t, rows, 1)
	row := rows[0]

	// Should have the selected columns
	assert.Equal(t, "test", row["txt"])
	assert.Equal(t, float64(1), row["i2"])
	assert.Equal(t, float64(2147483647), row["i4"])

	// Should NOT have other columns
	assert.NotContains(t, row, "i8")
	assert.NotContains(t, row, "f8")
	assert.NotContains(t, row, "b")
	assert.NotContains(t, row, "json")

	// Test update with column selection
	updateDone := make(chan bool)
	var finalRow goclient.Row

	unsubscribe := shape.Subscribe(func(value map[string]goclient.Row, rows []goclient.Row, diff goclient.Diff) {
		if len(rows) > 0 {
			finalRow = rows[0]
			if finalRow["txt"] == "changed" {
				updateDone <- true
			}
		}
	})
	defer unsubscribe()

	// Update the data
	err = table.UpdateTestData()
	require.NoError(t, err)

	// Wait for update
	select {
	case <-updateDone:
		// Success
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for column selection update")
	}

	// Verify updated data still only has selected columns
	assert.Equal(t, "changed", finalRow["txt"])
	assert.Equal(t, float64(1), finalRow["i2"])
	assert.Equal(t, float64(20), finalRow["i4"])

	// Should still NOT have other columns
	assert.NotContains(t, finalRow, "i8")
	assert.NotContains(t, finalRow, "f8")
}
