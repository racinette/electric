package goclient

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

// ParseFunction represents a function that can parse a string value into any type
type ParseFunction func(value string, additionalInfo ...*ColumnInfo) (Value, error)

// NullableParseFunction represents a function that can parse a string value or nil into any type
type NullableParseFunction func(value *string, additionalInfo ...*ColumnInfo) (Value, error)

// Parser is a map of PostgreSQL type names to their corresponding parse functions
type Parser map[string]ParseFunction

// parseNumber parses a string as a float64 (handles both integers and floats)
func parseNumber(value string, additionalInfo ...*ColumnInfo) (Value, error) {
	num, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse number: %w", err)
	}
	return num, nil
}

// parseInt parses a string as an integer
func parseInt(value string, additionalInfo ...*ColumnInfo) (Value, error) {
	num, err := strconv.Atoi(value)
	if err != nil {
		return nil, fmt.Errorf("failed to parse integer: %w", err)
	}
	return num, nil
}

// parseBigInt parses a string as a big integer (represented as *big.Int)
func parseBigInt(value string, additionalInfo ...*ColumnInfo) (Value, error) {
	num := new(big.Int)
	_, ok := num.SetString(value, 10)
	if !ok {
		return nil, fmt.Errorf("failed to parse big integer: %s", value)
	}
	return num, nil
}

// parseBool parses a string as a boolean (supports "t"/"true"/"f"/"false")
func parseBool(value string, additionalInfo ...*ColumnInfo) (Value, error) {
	switch value {
	case "t", "true":
		return true, nil
	case "f", "false":
		return false, nil
	default:
		return nil, fmt.Errorf("invalid boolean value: %s", value)
	}
}

// parseJSON parses a string as JSON
func parseJSON(value string, additionalInfo ...*ColumnInfo) (Value, error) {
	var result interface{}
	err := json.Unmarshal([]byte(value), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}
	return result, nil
}

// identityParser returns the string value as-is
func identityParser(value string, additionalInfo ...*ColumnInfo) (Value, error) {
	return value, nil
}

// DefaultParser provides the standard PostgreSQL type parsers
var DefaultParser = Parser{
	"int2":    parseInt,
	"int4":    parseInt,
	"int8":    parseBigInt,
	"bool":    parseBool,
	"float4":  parseNumber,
	"float8":  parseNumber,
	"json":    parseJSON,
	"jsonb":   parseJSON,
	"text":    identityParser,
	"varchar": identityParser,
	"char":    identityParser,
	"bpchar":  identityParser,
	"name":    identityParser,
	"uuid":    identityParser,
	"numeric": identityParser, // NUMERIC handled as string by default
}

// PgArrayParser parses PostgreSQL array format strings
func PgArrayParser(value string, parser NullableParseFunction) (Value, error) {
	if value == "" {
		return []interface{}{}, nil
	}

	result, _, err := parseArray(value, 0, parser)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// parseArray recursively parses PostgreSQL array format
func parseArray(value string, start int, parser NullableParseFunction) (interface{}, int, error) {
	if start >= len(value) {
		return nil, start, fmt.Errorf("unexpected end of array")
	}

	if value[start] != '{' {
		return nil, start, fmt.Errorf("expected '{' at position %d", start)
	}

	start++ // skip '{'
	result := make([]interface{}, 0)
	var str strings.Builder
	quoted := false
	i := start

	for i < len(value) {
		char := value[i]

		if quoted {
			if char == '\\' && i+1 < len(value) {
				// Handle escaped characters
				i++
				str.WriteByte(value[i])
			} else if char == '"' {
				// End of quoted string
				val := str.String()
				parsedVal, err := parser(&val)
				if err != nil {
					return nil, i, err
				}
				result = append(result, parsedVal)
				str.Reset()
				quoted = false
				// Skip until next delimiter
				for i+1 < len(value) && value[i+1] != ',' && value[i+1] != '}' {
					i++
				}
			} else {
				str.WriteByte(char)
			}
		} else {
			switch char {
			case '"':
				quoted = true
			case '{':
				// Nested array
				nestedResult, newPos, err := parseArray(value, i, parser)
				if err != nil {
					return nil, newPos, err
				}
				result = append(result, nestedResult)
				i = newPos - 1 // parseArray returns position after '}', so we subtract 1 since we'll increment at end of loop
				str.Reset()
			case '}':
				// End of array
				if str.Len() > 0 {
					val := strings.TrimSpace(str.String())
					var parsedVal interface{}
					var err error
					if val == "NULL" {
						parsedVal, err = parser(nil)
					} else {
						parsedVal, err = parser(&val)
					}
					if err != nil {
						return nil, i, err
					}
					result = append(result, parsedVal)
				}
				return result, i + 1, nil
			case ',':
				// Element separator
				if str.Len() > 0 {
					val := strings.TrimSpace(str.String())
					var parsedVal interface{}
					var err error
					if val == "NULL" {
						parsedVal, err = parser(nil)
					} else {
						parsedVal, err = parser(&val)
					}
					if err != nil {
						return nil, i, err
					}
					result = append(result, parsedVal)
					str.Reset()
				}
			case ' ':
				// Skip whitespace outside quotes
			default:
				str.WriteByte(char)
			}
		}
		i++
	}

	return nil, i, fmt.Errorf("unterminated array")
}

// rawChangeMessage is used for initial JSON parsing to preserve raw JSON data
// for value and old_value fields, preventing automatic float64 conversion
type rawChangeMessage struct {
	Key      string          `json:"key"`
	Value    json.RawMessage `json:"value"`
	OldValue json.RawMessage `json:"old_value,omitempty"`
	Headers  struct {
		Operation Operation `json:"operation"`
	} `json:"headers"`
}

// ParseMessages parses a JSON array of messages using schema for value parsing.
// This function provides schema-aware type conversion for message values while
// maintaining the structured Message types that the codebase depends on.
func ParseMessages(data []byte, schema Schema, customParser ...Parser) ([]Message, error) {
	// Set up parser for schema-aware parsing
	var parser Parser
	if len(customParser) > 0 {
		// Merge custom parser with default parser
		parser = make(Parser)
		for k, v := range DefaultParser {
			parser[k] = v
		}
		for k, v := range customParser[0] {
			parser[k] = v
		}
	} else {
		parser = DefaultParser
	}

	// We decode into raw slice to differentiate control vs change
	var rawMsgs []json.RawMessage
	if err := json.Unmarshal(data, &rawMsgs); err != nil {
		return nil, err
	}

	out := make([]Message, 0, len(rawMsgs))
	for _, rm := range rawMsgs {
		// try change (has key)
		var probe map[string]any
		if err := json.Unmarshal(rm, &probe); err != nil {
			return nil, err
		}
		if _, ok := probe["key"]; ok {
			// Parse into raw change message to preserve raw JSON for value fields
			var rawCh rawChangeMessage
			if err := json.Unmarshal(rm, &rawCh); err != nil {
				return nil, err
			}

			// Create final change message
			ch := ChangeMessage{
				Key:     rawCh.Key,
				Headers: rawCh.Headers,
			}

			// Parse value field if present
			if len(rawCh.Value) > 0 && string(rawCh.Value) != "null" {
				if schema != nil {
					parsedValue, err := parseRawRowWithSchema(rawCh.Value, schema, parser)
					if err != nil {
						return nil, err
					}
					ch.Value = parsedValue
				} else {
					// Fallback to regular JSON parsing when no schema
					var value Row
					if err := json.Unmarshal(rawCh.Value, &value); err != nil {
						return nil, err
					}
					ch.Value = value
				}
			}

			// Parse old_value field if present
			if len(rawCh.OldValue) > 0 && string(rawCh.OldValue) != "null" {
				if schema != nil {
					parsedOldValue, err := parseRawRowWithSchema(rawCh.OldValue, schema, parser)
					if err != nil {
						return nil, err
					}
					ch.OldValue = parsedOldValue
				} else {
					// Fallback to regular JSON parsing when no schema
					var oldValue Row
					if err := json.Unmarshal(rawCh.OldValue, &oldValue); err != nil {
						return nil, err
					}
					ch.OldValue = oldValue
				}
			}

			out = append(out, Message{Change: &ch})
			continue
		}
		var ctrl ControlMessage
		if err := json.Unmarshal(rm, &ctrl); err != nil {
			return nil, err
		}
		out = append(out, Message{Control: &ctrl})
	}
	return out, nil
}

// parseRawRowWithSchema parses a raw JSON row using schema information.
// This function avoids the intermediate conversion to map[string]interface{} that
// would convert all numbers to float64, preserving type information from the schema.
func parseRawRowWithSchema(rawData json.RawMessage, schema Schema, parser Parser) (Row, error) {
	// First, unmarshal into a map[string]json.RawMessage to get field-level raw data
	var rawFields map[string]json.RawMessage
	if err := json.Unmarshal(rawData, &rawFields); err != nil {
		return nil, err
	}

	result := make(Row)
	for key, rawValue := range rawFields {
		columnInfo, exists := schema[key]
		if !exists {
			// No schema information, fallback to regular JSON parsing
			var value interface{}
			if err := json.Unmarshal(rawValue, &value); err != nil {
				return nil, err
			}
			result[key] = value
			continue
		}

		// Parse the raw value using schema information
		parsedValue, err := parseRawValueWithSchema(key, rawValue, columnInfo, parser)
		if err != nil {
			return nil, err
		}
		result[key] = parsedValue
	}

	return result, nil
}

// parseRawValueWithSchema parses a single raw JSON value based on column information
func parseRawValueWithSchema(columnName string, rawValue json.RawMessage, columnInfo ColumnInfo, parser Parser) (interface{}, error) {
	// Handle null values
	if string(rawValue) == "null" {
		isNullable := true
		if columnInfo.NotNull != nil {
			isNullable = !*columnInfo.NotNull
		}
		if !isNullable {
			return nil, ParserNullValueError{ColumnName: columnName}
		}
		return nil, nil
	}

	// Get the parser for this type
	typeParser, exists := parser[columnInfo.Type]
	if !exists {
		// No parser for this type, fallback to regular JSON parsing
		var value interface{}
		if err := json.Unmarshal(rawValue, &value); err != nil {
			return nil, err
		}
		return value, nil
	}

	// Convert raw JSON to string for parsing
	var strValue string
	if err := json.Unmarshal(rawValue, &strValue); err != nil {
		// If it's not a string, fallback to regular JSON parsing
		// This handles cases like JSON objects/arrays that should be parsed as-is
		var value interface{}
		if err := json.Unmarshal(rawValue, &value); err != nil {
			return nil, err
		}
		return value, nil
	}

	// Create nullable parser
	nullableParser := makeNullableParser(typeParser, columnInfo, columnName)

	// Handle arrays
	if columnInfo.Dims != nil && *columnInfo.Dims > 0 {
		arrayParser := func(value *string, additionalInfo ...*ColumnInfo) (Value, error) {
			if value == nil {
				if columnInfo.NotNull != nil && *columnInfo.NotNull {
					return nil, ParserNullValueError{ColumnName: columnName}
				}
				return nil, nil
			}
			return PgArrayParser(*value, nullableParser)
		}

		arrayNullableParser := makeNullableParser(func(value string, additionalInfo ...*ColumnInfo) (Value, error) {
			return arrayParser(&value, additionalInfo...)
		}, columnInfo, columnName)

		return arrayNullableParser(&strValue, &columnInfo)
	}

	// Parse single value
	return nullableParser(&strValue, &columnInfo)
}

// parseRowWithSchema parses a row using schema information, similar to MessageParser.parseRow
func parseRowWithSchema(row Row, schema Schema, parser Parser) (Row, error) {
	result := make(Row)

	for key, value := range row {
		columnInfo, exists := schema[key]
		if !exists {
			// No schema information, keep value as-is
			result[key] = value
			continue
		}

		parsedValue, err := parseValueWithSchema(key, value, columnInfo, parser)
		if err != nil {
			return nil, err
		}
		result[key] = parsedValue
	}

	return result, nil
}

// parseValueWithSchema parses a single value based on column information, similar to MessageParser.parseValue
func parseValueWithSchema(columnName string, value interface{}, columnInfo ColumnInfo, parser Parser) (interface{}, error) {
	// Handle null values
	if value == nil {
		isNullable := true
		if columnInfo.NotNull != nil {
			isNullable = !*columnInfo.NotNull
		}
		if !isNullable {
			return nil, ParserNullValueError{ColumnName: columnName}
		}
		return nil, nil
	}

	// Convert value to string for parsing
	var strValue string
	switch v := value.(type) {
	case string:
		strValue = v
	case *string:
		if v == nil {
			if columnInfo.NotNull != nil && *columnInfo.NotNull {
				return nil, ParserNullValueError{ColumnName: columnName}
			}
			return nil, nil
		}
		strValue = *v
	default:
		// For non-string values, return as-is (e.g., already parsed JSON values)
		return value, nil
	}

	// Get the parser for this type
	typeParser, exists := parser[columnInfo.Type]
	if !exists {
		// No parser for this type, return as-is
		return strValue, nil
	}

	// Create nullable parser
	nullableParser := makeNullableParser(typeParser, columnInfo, columnName)

	// Handle arrays
	if columnInfo.Dims != nil && *columnInfo.Dims > 0 {
		arrayParser := func(value *string, additionalInfo ...*ColumnInfo) (Value, error) {
			if value == nil {
				if columnInfo.NotNull != nil && *columnInfo.NotNull {
					return nil, ParserNullValueError{ColumnName: columnName}
				}
				return nil, nil
			}
			return PgArrayParser(*value, nullableParser)
		}

		arrayNullableParser := makeNullableParser(func(value string, additionalInfo ...*ColumnInfo) (Value, error) {
			return arrayParser(&value, additionalInfo...)
		}, columnInfo, columnName)

		return arrayNullableParser(&strValue, &columnInfo)
	}

	// Parse single value
	return nullableParser(&strValue, &columnInfo)
}

// makeNullableParser creates a nullable version of a parse function, similar to MessageParser.makeNullableParser
func makeNullableParser(parser ParseFunction, columnInfo ColumnInfo, columnName string) NullableParseFunction {
	return func(value *string, additionalInfo ...*ColumnInfo) (Value, error) {
		if value == nil {
			isNullable := true
			if columnInfo.NotNull != nil {
				isNullable = !*columnInfo.NotNull
			}
			if !isNullable {
				return nil, ParserNullValueError{ColumnName: columnName}
			}
			return nil, nil
		}
		return parser(*value, &columnInfo)
	}
}
