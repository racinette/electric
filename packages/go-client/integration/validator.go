package integration

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	goclient "github.com/electric-sql/electric/packages/go-client"
)

// ShapeValidator validates that a shape's state matches the actual database state
type ShapeValidator struct {
	dbClient    *TestDBClient
	table       *TestIssuesTable
	whereClause string
	columns     []string
}

// NewShapeValidator creates a new validator for comparing shape state with database state
func NewShapeValidator(dbClient *TestDBClient, table *TestIssuesTable, whereClause string, columns []string) *ShapeValidator {
	return &ShapeValidator{
		dbClient:    dbClient,
		table:       table,
		whereClause: whereClause,
		columns:     columns,
	}
}

// ValidateShapeState compares the current shape state with the actual database state
func (v *ShapeValidator) ValidateShapeState(shape *goclient.Shape) error {
	// Get current shape data
	shapeRows := shape.CurrentRows()

	// Query database with the same criteria as the shape
	dbRows, err := v.queryDatabaseState()
	if err != nil {
		return fmt.Errorf("failed to query database state: %w", err)
	}

	// Convert both to comparable format
	shapeData := v.normalizeShapeRows(shapeRows)
	dbData := v.normalizeDbRows(dbRows)

	// Compare row counts
	if len(shapeData) != len(dbData) {
		return fmt.Errorf("row count mismatch: shape has %d rows, database has %d rows",
			len(shapeData), len(dbData))
	}

	// Compare individual rows
	for id, shapeRow := range shapeData {
		dbRow, exists := dbData[id]
		if !exists {
			return fmt.Errorf("shape contains row with id %s that doesn't exist in database", id)
		}

		if err := v.compareRows(id, shapeRow, dbRow); err != nil {
			return err
		}
	}

	// Check for database rows not in shape
	for id := range dbData {
		if _, exists := shapeData[id]; !exists {
			return fmt.Errorf("database contains row with id %s that doesn't exist in shape", id)
		}
	}

	return nil
}

// queryDatabaseState queries the database using the same criteria as the shape
func (v *ShapeValidator) queryDatabaseState() ([]map[string]interface{}, error) {
	// Build query
	var query string
	var args []interface{}

	if len(v.columns) > 0 {
		// Specific columns requested
		columnList := strings.Join(v.columns, ", ")
		query = fmt.Sprintf("SELECT %s FROM %s", columnList, v.table.TableName())
	} else {
		// All columns
		query = fmt.Sprintf("SELECT id, title, priority FROM %s", v.table.TableName())
	}

	if v.whereClause != "" {
		query += " WHERE " + v.whereClause
	}

	query += " ORDER BY id" // Ensure consistent ordering

	// Execute query
	rows, err := v.dbClient.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Get column info
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Read all rows
	var result []map[string]interface{}
	for rows.Next() {
		// Create slice of interface{} for Scan
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert to map
		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}
		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during row iteration: %w", err)
	}

	return result, nil
}

// normalizeShapeRows converts shape rows to a comparable format
func (v *ShapeValidator) normalizeShapeRows(rows []goclient.Row) map[string]map[string]interface{} {
	result := make(map[string]map[string]interface{})

	for _, row := range rows {
		idVal, ok := row["id"]
		if !ok {
			continue
		}

		id := fmt.Sprintf("%v", idVal)
		normalizedRow := make(map[string]interface{})

		for key, value := range row {
			normalizedRow[key] = v.normalizeValue(value)
		}

		result[id] = normalizedRow
	}

	return result
}

// normalizeDbRows converts database rows to a comparable format
func (v *ShapeValidator) normalizeDbRows(rows []map[string]interface{}) map[string]map[string]interface{} {
	result := make(map[string]map[string]interface{})

	for _, row := range rows {
		idVal, ok := row["id"]
		if !ok {
			continue
		}

		id := fmt.Sprintf("%v", idVal)
		normalizedRow := make(map[string]interface{})

		for key, value := range row {
			normalizedRow[key] = v.normalizeValue(value)
		}

		result[id] = normalizedRow
	}

	return result
}

// normalizeValue normalizes values for comparison between shape and database
func (v *ShapeValidator) normalizeValue(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case []byte:
		// Convert byte slices to strings for comparison
		return string(v)
	case int64:
		// Convert to float64 for consistency with JSON numbers
		return float64(v)
	case int32:
		return float64(v)
	case int:
		return float64(v)
	case float32:
		return float64(v)
	default:
		return value
	}
}

// compareRows compares two normalized rows
func (v *ShapeValidator) compareRows(id string, shapeRow, dbRow map[string]interface{}) error {
	// Check that shape row has all the expected fields
	for key, dbValue := range dbRow {
		shapeValue, exists := shapeRow[key]
		if !exists {
			return fmt.Errorf("row %s: shape missing field %s", id, key)
		}

		if !v.valuesEqual(shapeValue, dbValue) {
			return fmt.Errorf("row %s: field %s mismatch: shape=%v, db=%v",
				id, key, shapeValue, dbValue)
		}
	}

	// Check that shape doesn't have unexpected fields (unless we selected specific columns)
	if len(v.columns) == 0 {
		for key := range shapeRow {
			if _, exists := dbRow[key]; !exists {
				return fmt.Errorf("row %s: shape has unexpected field %s", id, key)
			}
		}
	}

	return nil
}

// valuesEqual compares two values for equality, handling type differences
func (v *ShapeValidator) valuesEqual(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Handle string comparisons
	if aStr, aOk := a.(string); aOk {
		if bStr, bOk := b.(string); bOk {
			return aStr == bStr
		}
		// Try converting b to string
		return aStr == fmt.Sprintf("%v", b)
	}

	// Handle numeric comparisons
	aFloat := v.toFloat64(a)
	bFloat := v.toFloat64(b)
	if aFloat != nil && bFloat != nil {
		return *aFloat == *bFloat
	}

	// Handle boolean comparisons
	if aBool, aOk := a.(bool); aOk {
		if bBool, bOk := b.(bool); bOk {
			return aBool == bBool
		}
	}

	// Fallback to reflect.DeepEqual
	return reflect.DeepEqual(a, b)
}

// toFloat64 attempts to convert a value to float64
func (v *ShapeValidator) toFloat64(value interface{}) *float64 {
	switch v := value.(type) {
	case float64:
		return &v
	case float32:
		f := float64(v)
		return &f
	case int:
		f := float64(v)
		return &f
	case int32:
		f := float64(v)
		return &f
	case int64:
		f := float64(v)
		return &f
	default:
		return nil
	}
}

// QueryShapeEquivalent queries the database using the same criteria as a shape
// This is useful for debugging shape validation issues
func (v *ShapeValidator) QueryShapeEquivalent() ([]map[string]interface{}, error) {
	return v.queryDatabaseState()
}

// CompareShapeWithDB is a convenience method that returns detailed comparison info
func (v *ShapeValidator) CompareShapeWithDB(shape *goclient.Shape) (ComparisonResult, error) {
	result := ComparisonResult{}

	// Get both datasets
	shapeRows := shape.CurrentRows()
	dbRows, err := v.queryDatabaseState()
	if err != nil {
		return result, fmt.Errorf("failed to query database: %w", err)
	}

	result.ShapeRowCount = len(shapeRows)
	result.DatabaseRowCount = len(dbRows)

	// Normalize data
	shapeData := v.normalizeShapeRows(shapeRows)
	dbData := v.normalizeDbRows(dbRows)

	// Find differences
	for id, shapeRow := range shapeData {
		if dbRow, exists := dbData[id]; exists {
			if err := v.compareRows(id, shapeRow, dbRow); err != nil {
				result.RowDifferences = append(result.RowDifferences, err.Error())
			}
		} else {
			result.OnlyInShape = append(result.OnlyInShape, id)
		}
	}

	for id := range dbData {
		if _, exists := shapeData[id]; !exists {
			result.OnlyInDatabase = append(result.OnlyInDatabase, id)
		}
	}

	// Sort for consistent output
	sort.Strings(result.OnlyInShape)
	sort.Strings(result.OnlyInDatabase)
	sort.Strings(result.RowDifferences)

	result.IsEqual = len(result.OnlyInShape) == 0 &&
		len(result.OnlyInDatabase) == 0 &&
		len(result.RowDifferences) == 0

	return result, nil
}

// ComparisonResult holds detailed comparison information
type ComparisonResult struct {
	IsEqual          bool
	ShapeRowCount    int
	DatabaseRowCount int
	OnlyInShape      []string
	OnlyInDatabase   []string
	RowDifferences   []string
}

// String returns a human-readable summary of the comparison
func (r ComparisonResult) String() string {
	if r.IsEqual {
		return fmt.Sprintf("Shape and database are equal (%d rows)", r.ShapeRowCount)
	}

	var parts []string
	parts = append(parts, fmt.Sprintf("Shape: %d rows, Database: %d rows",
		r.ShapeRowCount, r.DatabaseRowCount))

	if len(r.OnlyInShape) > 0 {
		parts = append(parts, fmt.Sprintf("Only in shape: %v", r.OnlyInShape))
	}
	if len(r.OnlyInDatabase) > 0 {
		parts = append(parts, fmt.Sprintf("Only in database: %v", r.OnlyInDatabase))
	}
	if len(r.RowDifferences) > 0 {
		parts = append(parts, fmt.Sprintf("Row differences: %v", r.RowDifferences))
	}

	return strings.Join(parts, "; ")
}
