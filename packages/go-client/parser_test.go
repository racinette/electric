package goclient

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultParser(t *testing.T) {
	t.Run("should parse integers", func(t *testing.T) {
		parser := DefaultParser

		// Test int2
		result, err := parser["int2"]("0")
		require.NoError(t, err)
		assert.Equal(t, 0, result)

		result, err = parser["int2"]("-32768")
		require.NoError(t, err)
		assert.Equal(t, -32768, result)

		result, err = parser["int2"]("32767")
		require.NoError(t, err)
		assert.Equal(t, 32767, result)

		// Test int4
		result, err = parser["int4"]("0")
		require.NoError(t, err)
		assert.Equal(t, 0, result)

		result, err = parser["int4"]("2147483647")
		require.NoError(t, err)
		assert.Equal(t, 2147483647, result)

		result, err = parser["int4"]("-2147483648")
		require.NoError(t, err)
		assert.Equal(t, -2147483648, result)
	})

	t.Run("should parse bigints", func(t *testing.T) {
		parser := DefaultParser

		result, err := parser["int8"]("-9223372036854775808")
		require.NoError(t, err)
		expected := new(big.Int)
		expected.SetString("-9223372036854775808", 10)
		assert.Equal(t, expected, result)

		result, err = parser["int8"]("9223372036854775807")
		require.NoError(t, err)
		expected = new(big.Int)
		expected.SetString("9223372036854775807", 10)
		assert.Equal(t, expected, result)

		result, err = parser["int8"]("0")
		require.NoError(t, err)
		expected = new(big.Int)
		expected.SetString("0", 10)
		assert.Equal(t, expected, result)
	})

	t.Run("should parse booleans", func(t *testing.T) {
		parser := DefaultParser

		result, err := parser["bool"]("t")
		require.NoError(t, err)
		assert.Equal(t, true, result)

		result, err = parser["bool"]("true")
		require.NoError(t, err)
		assert.Equal(t, true, result)

		result, err = parser["bool"]("false")
		require.NoError(t, err)
		assert.Equal(t, false, result)

		result, err = parser["bool"]("f")
		require.NoError(t, err)
		assert.Equal(t, false, result)
	})

	t.Run("should parse float4", func(t *testing.T) {
		parser := DefaultParser

		result, err := parser["float4"]("1.1754944e-38")
		require.NoError(t, err)
		assert.InDelta(t, 1.1754944e-38, result, 1e-45)

		result, err = parser["float4"]("3.4028235e38")
		require.NoError(t, err)
		assert.InDelta(t, 3.4028235e38, result, 1e30)

		result, err = parser["float4"]("-3.4028235e38")
		require.NoError(t, err)
		assert.InDelta(t, -3.4028235e38, result, 1e30)

		result, err = parser["float4"]("-1.1754944e-38")
		require.NoError(t, err)
		assert.InDelta(t, -1.1754944e-38, result, 1e-45)

		result, err = parser["float4"]("0")
		require.NoError(t, err)
		assert.Equal(t, float64(0), result)

		result, err = parser["float4"]("Infinity")
		require.NoError(t, err)
		assert.True(t, result.(float64) > 0 && result.(float64) == result.(float64)+1) // Check for positive infinity

		result, err = parser["float4"]("-Infinity")
		require.NoError(t, err)
		assert.True(t, result.(float64) < 0 && result.(float64) == result.(float64)+1) // Check for negative infinity

		result, err = parser["float4"]("NaN")
		require.NoError(t, err)
		assert.True(t, result.(float64) != result.(float64)) // Check for NaN
	})

	t.Run("should parse float8", func(t *testing.T) {
		parser := DefaultParser

		result, err := parser["float8"]("1.797e308")
		require.NoError(t, err)
		assert.InDelta(t, 1.797e308, result, 1e300)

		result, err = parser["float8"]("-1.797e+308")
		require.NoError(t, err)
		assert.InDelta(t, -1.797e+308, result, 1e300)

		result, err = parser["float8"]("0")
		require.NoError(t, err)
		assert.Equal(t, float64(0), result)

		result, err = parser["float8"]("Infinity")
		require.NoError(t, err)
		assert.True(t, result.(float64) > 0 && result.(float64) == result.(float64)+1) // Check for positive infinity

		result, err = parser["float8"]("-Infinity")
		require.NoError(t, err)
		assert.True(t, result.(float64) < 0 && result.(float64) == result.(float64)+1) // Check for negative infinity

		result, err = parser["float8"]("NaN")
		require.NoError(t, err)
		assert.True(t, result.(float64) != result.(float64)) // Check for NaN
	})

	t.Run("should parse json", func(t *testing.T) {
		parser := DefaultParser

		result, err := parser["json"]("true")
		require.NoError(t, err)
		assert.Equal(t, true, result)

		result, err = parser["json"]("5")
		require.NoError(t, err)
		assert.Equal(t, float64(5), result)

		result, err = parser["json"](`"foo"`)
		require.NoError(t, err)
		assert.Equal(t, "foo", result)

		result, err = parser["json"]("{}")
		require.NoError(t, err)
		assert.Equal(t, map[string]interface{}{}, result)

		result, err = parser["json"]("null")
		require.NoError(t, err)
		assert.Nil(t, result)

		result, err = parser["json"](`{"a":null}`)
		require.NoError(t, err)
		expected := map[string]interface{}{"a": nil}
		assert.Equal(t, expected, result)

		result, err = parser["json"]("[]")
		require.NoError(t, err)
		assert.Equal(t, []interface{}{}, result)

		result, err = parser["json"](`{"a":1}`)
		require.NoError(t, err)
		expected = map[string]interface{}{"a": float64(1)}
		assert.Equal(t, expected, result)

		result, err = parser["json"](`{"a":1,"b":2}`)
		require.NoError(t, err)
		expected = map[string]interface{}{"a": float64(1), "b": float64(2)}
		assert.Equal(t, expected, result)

		result, err = parser["json"](`[{"a":1,"b":2},{"c": [{"d": 5}]}]`)
		require.NoError(t, err)
		expectedArray := []interface{}{
			map[string]interface{}{"a": float64(1), "b": float64(2)},
			map[string]interface{}{"c": []interface{}{map[string]interface{}{"d": float64(5)}}},
		}
		assert.Equal(t, expectedArray, result)

		// Test jsonb
		result, err = parser["jsonb"]("true")
		require.NoError(t, err)
		assert.Equal(t, true, result)

		result, err = parser["jsonb"]("5")
		require.NoError(t, err)
		assert.Equal(t, float64(5), result)

		result, err = parser["jsonb"](`"foo"`)
		require.NoError(t, err)
		assert.Equal(t, "foo", result)

		result, err = parser["jsonb"]("{}")
		require.NoError(t, err)
		assert.Equal(t, map[string]interface{}{}, result)

		result, err = parser["jsonb"]("null")
		require.NoError(t, err)
		assert.Nil(t, result)

		result, err = parser["jsonb"](`{"a":null}`)
		require.NoError(t, err)
		expected = map[string]interface{}{"a": nil}
		assert.Equal(t, expected, result)

		result, err = parser["jsonb"]("[]")
		require.NoError(t, err)
		assert.Equal(t, []interface{}{}, result)

		result, err = parser["jsonb"](`{"a":1}`)
		require.NoError(t, err)
		expected = map[string]interface{}{"a": float64(1)}
		assert.Equal(t, expected, result)

		result, err = parser["jsonb"](`{"a":1,"b":2}`)
		require.NoError(t, err)
		expected = map[string]interface{}{"a": float64(1), "b": float64(2)}
		assert.Equal(t, expected, result)

		result, err = parser["jsonb"](`[{"a":1,"b":2},{"c": [{"d": 5}]}]`)
		require.NoError(t, err)
		expectedArrayJsonb := []interface{}{
			map[string]interface{}{"a": float64(1), "b": float64(2)},
			map[string]interface{}{"c": []interface{}{map[string]interface{}{"d": float64(5)}}},
		}
		assert.Equal(t, expectedArrayJsonb, result)
	})

	t.Run("should parse numeric as string", func(t *testing.T) {
		parser := DefaultParser

		result, err := parser["numeric"]("123.456")
		require.NoError(t, err)
		assert.Equal(t, "123.456", result)

		result, err = parser["numeric"]("999999999999999999999.999999999")
		require.NoError(t, err)
		assert.Equal(t, "999999999999999999999.999999999", result)
	})

	t.Run("should parse text types", func(t *testing.T) {
		parser := DefaultParser

		result, err := parser["text"]("hello world")
		require.NoError(t, err)
		assert.Equal(t, "hello world", result)

		result, err = parser["varchar"]("varchar value")
		require.NoError(t, err)
		assert.Equal(t, "varchar value", result)

		result, err = parser["uuid"]("550e8400-e29b-41d4-a716-446655440000")
		require.NoError(t, err)
		assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", result)
	})
}

func TestPostgresArrayParser(t *testing.T) {
	// Create nullable parser helpers similar to TypeScript tests
	nullableIntParser := func(val *string, additionalInfo ...*ColumnInfo) (Value, error) {
		if val == nil {
			return nil, nil
		}
		return parseInt(*val)
	}

	nullableBigIntParser := func(val *string, additionalInfo ...*ColumnInfo) (Value, error) {
		if val == nil {
			return nil, nil
		}
		return parseBigInt(*val)
	}

	nullableBoolParser := func(val *string, additionalInfo ...*ColumnInfo) (Value, error) {
		if val == nil {
			return nil, nil
		}
		return parseBool(*val)
	}

	nullableFloat8Parser := func(val *string, additionalInfo ...*ColumnInfo) (Value, error) {
		if val == nil {
			return nil, nil
		}
		return parseNumber(*val)
	}

	nullableJSONParser := func(val *string, additionalInfo ...*ColumnInfo) (Value, error) {
		if val == nil {
			return nil, nil
		}
		return parseJSON(*val)
	}

	identityNullableParser := func(val *string, additionalInfo ...*ColumnInfo) (Value, error) {
		if val == nil {
			return nil, nil
		}
		return *val, nil
	}

	t.Run("should parse arrays and their values", func(t *testing.T) {
		result, err := PgArrayParser("{1,2,3,4,5,NULL}", nullableIntParser)
		require.NoError(t, err)
		expected := []interface{}{1, 2, 3, 4, 5, nil}
		assert.Equal(t, expected, result)

		result, err = PgArrayParser("{1,2,3,4,5,NULL}", nullableBigIntParser)
		require.NoError(t, err)
		expectedBigInt := []interface{}{
			mustParseBigInt("1"),
			mustParseBigInt("2"),
			mustParseBigInt("3"),
			mustParseBigInt("4"),
			mustParseBigInt("5"),
			nil,
		}
		assert.Equal(t, expectedBigInt, result)

		result, err = PgArrayParser(`{"foo","bar","NULL",NULL}`, identityNullableParser)
		require.NoError(t, err)
		expected = []interface{}{"foo", "bar", "NULL", nil}
		assert.Equal(t, expected, result)

		result, err = PgArrayParser(`{foo,"}"}`, identityNullableParser)
		require.NoError(t, err)
		expected = []interface{}{"foo", "}"}
		assert.Equal(t, expected, result)

		result, err = PgArrayParser("{t,f,f,NULL}", nullableBoolParser)
		require.NoError(t, err)
		expected = []interface{}{true, false, false, nil}
		assert.Equal(t, expected, result)

		result, err = PgArrayParser("{}", nullableJSONParser)
		require.NoError(t, err)
		assert.Equal(t, []interface{}{}, result)

		result, err = PgArrayParser(`{"{}"}`, nullableJSONParser)
		require.NoError(t, err)
		expected = []interface{}{map[string]interface{}{}}
		assert.Equal(t, expected, result)

		result, err = PgArrayParser("{null}", nullableJSONParser)
		require.NoError(t, err)
		expected = []interface{}{nil}
		assert.Equal(t, expected, result)

		// Test JSON object with escaped quotes
		result, err = PgArrayParser(`{"{\"a\":null}"}`, nullableJSONParser)
		require.NoError(t, err)
		expected = []interface{}{map[string]interface{}{"a": nil}}
		assert.Equal(t, expected, result)

		result, err = PgArrayParser("{Infinity,-Infinity,NaN,NULL}", nullableFloat8Parser)
		require.NoError(t, err)
		resultArr := result.([]interface{})
		assert.Len(t, resultArr, 4)
		assert.True(t, resultArr[0].(float64) > 0 && resultArr[0].(float64) == resultArr[0].(float64)+1) // Positive infinity
		assert.True(t, resultArr[1].(float64) < 0 && resultArr[1].(float64) == resultArr[1].(float64)+1) // Negative infinity
		assert.True(t, resultArr[2].(float64) != resultArr[2].(float64))                                 // NaN
		assert.Nil(t, resultArr[3])                                                                      // NULL
	})

	t.Run("should parse nested arrays", func(t *testing.T) {
		result, err := PgArrayParser("{{1,2},{3,4}}", nullableIntParser)
		require.NoError(t, err)
		expected := []interface{}{
			[]interface{}{1, 2},
			[]interface{}{3, 4},
		}
		assert.Equal(t, expected, result)

		result, err = PgArrayParser(`{{"foo"},{"bar"}}`, identityNullableParser)
		require.NoError(t, err)
		expected = []interface{}{
			[]interface{}{"foo"},
			[]interface{}{"bar"},
		}
		assert.Equal(t, expected, result)

		result, err = PgArrayParser("{{t,f}, {f,t}}", nullableBoolParser)
		require.NoError(t, err)
		expected = []interface{}{
			[]interface{}{true, false},
			[]interface{}{false, true},
		}
		assert.Equal(t, expected, result)

		result, err = PgArrayParser("{{1,2},{3,4}}", nullableBigIntParser)
		require.NoError(t, err)
		expected = []interface{}{
			[]interface{}{mustParseBigInt("1"), mustParseBigInt("2")},
			[]interface{}{mustParseBigInt("3"), mustParseBigInt("4")},
		}
		assert.Equal(t, expected, result)

		result, err = PgArrayParser("{{},{}}", nullableJSONParser)
		require.NoError(t, err)
		resultArr := result.([]interface{})
		assert.Len(t, resultArr, 2)
		assert.Equal(t, []interface{}{}, resultArr[0])
		assert.Equal(t, []interface{}{}, resultArr[1])

		result, err = PgArrayParser(`{"{}","{}"}`, nullableJSONParser)
		require.NoError(t, err)
		expected = []interface{}{
			map[string]interface{}{},
			map[string]interface{}{},
		}
		assert.Equal(t, expected, result)

		result, err = PgArrayParser("{null,null}", nullableJSONParser)
		require.NoError(t, err)
		expected = []interface{}{nil, nil}
		assert.Equal(t, expected, result)

		result, err = PgArrayParser(`{"{\"a\":null}", "{\"b\":null}"}`, nullableJSONParser)
		require.NoError(t, err)
		expected = []interface{}{
			map[string]interface{}{"a": nil},
			map[string]interface{}{"b": nil},
		}
		assert.Equal(t, expected, result)

		result, err = PgArrayParser("{{Infinity,-Infinity,NaN},{NaN,Infinity,-Infinity}}", nullableFloat8Parser)
		require.NoError(t, err)
		resultArrNested := result.([]interface{})
		assert.Len(t, resultArrNested, 2)

		firstNested := resultArrNested[0].([]interface{})
		assert.Len(t, firstNested, 3)
		assert.True(t, firstNested[0].(float64) > 0 && firstNested[0].(float64) == firstNested[0].(float64)+1) // Positive infinity
		assert.True(t, firstNested[1].(float64) < 0 && firstNested[1].(float64) == firstNested[1].(float64)+1) // Negative infinity
		assert.True(t, firstNested[2].(float64) != firstNested[2].(float64))                                   // NaN

		secondNested := resultArrNested[1].([]interface{})
		assert.Len(t, secondNested, 3)
		assert.True(t, secondNested[0].(float64) != secondNested[0].(float64))                                    // NaN
		assert.True(t, secondNested[1].(float64) > 0 && secondNested[1].(float64) == secondNested[1].(float64)+1) // Positive infinity
		assert.True(t, secondNested[2].(float64) < 0 && secondNested[2].(float64) == secondNested[2].(float64)+1) // Negative infinity
	})
}

func TestSchemaAwareParsing(t *testing.T) {
	t.Run("should parse null values with schema awareness", func(t *testing.T) {
		// Test data with change messages containing null values
		messages := `[
			{ "key": "test1", "value": { "a": null }, "headers": {"operation": "insert"} },
			{ "key": "test2", "value": { "value": null }, "headers": {"operation": "insert"} }
		]`

		schema := func(tpe string, dims *int) Schema {
			return Schema{
				"a":     {Type: tpe, Dims: dims},
				"value": {Type: tpe, Dims: dims},
			}
		}

		sampleDims := []*int{nil, intPtr(1), intPtr(2)}

		for _, dims := range sampleDims {
			// If it's not nullable it should throw
			notNullSchema := Schema{
				"a": {Type: "int2", Dims: dims, NotNull: boolPtr(true)},
			}
			_, err := ParseMessages([]byte(messages), notNullSchema)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), `Column "a" does not allow NULL values`)

			notNullSchema = Schema{
				"value": {Type: "int2", Dims: dims, NotNull: boolPtr(true)},
			}
			_, err = ParseMessages([]byte(messages), notNullSchema)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), `Column "value" does not allow NULL`)

			// Otherwise, it should parse as null
			result, err := ParseMessages([]byte(messages), schema("int2", dims))
			require.NoError(t, err)
			require.Len(t, result, 2)
			assert.True(t, IsChangeMessage(result[0]))
			assert.True(t, IsChangeMessage(result[1]))
			assert.Nil(t, result[0].Change.Value["a"])
			assert.Nil(t, result[1].Change.Value["value"])

			result, err = ParseMessages([]byte(messages), schema("int4", dims))
			require.NoError(t, err)
			require.Len(t, result, 2)
			assert.Nil(t, result[0].Change.Value["a"])
			assert.Nil(t, result[1].Change.Value["value"])

			result, err = ParseMessages([]byte(messages), schema("int8", dims))
			require.NoError(t, err)
			require.Len(t, result, 2)
			assert.Nil(t, result[0].Change.Value["a"])
			assert.Nil(t, result[1].Change.Value["value"])

			result, err = ParseMessages([]byte(messages), schema("bool", dims))
			require.NoError(t, err)
			require.Len(t, result, 2)
			assert.Nil(t, result[0].Change.Value["a"])
			assert.Nil(t, result[1].Change.Value["value"])

			result, err = ParseMessages([]byte(messages), schema("float4", dims))
			require.NoError(t, err)
			require.Len(t, result, 2)
			assert.Nil(t, result[0].Change.Value["a"])
			assert.Nil(t, result[1].Change.Value["value"])

			result, err = ParseMessages([]byte(messages), schema("float8", dims))
			require.NoError(t, err)
			require.Len(t, result, 2)
			assert.Nil(t, result[0].Change.Value["a"])
			assert.Nil(t, result[1].Change.Value["value"])

			result, err = ParseMessages([]byte(messages), schema("json", dims))
			require.NoError(t, err)
			require.Len(t, result, 2)
			assert.Nil(t, result[0].Change.Value["a"])
			assert.Nil(t, result[1].Change.Value["value"])

			result, err = ParseMessages([]byte(messages), schema("jsonb", dims))
			require.NoError(t, err)
			require.Len(t, result, 2)
			assert.Nil(t, result[0].Change.Value["a"])
			assert.Nil(t, result[1].Change.Value["value"])

			result, err = ParseMessages([]byte(messages), schema("text", dims))
			require.NoError(t, err)
			require.Len(t, result, 2)
			assert.Nil(t, result[0].Change.Value["a"])
			assert.Nil(t, result[1].Change.Value["value"])
		}
	})

	t.Run("should parse text values like 'NULL' correctly", func(t *testing.T) {
		schema := Schema{
			"a": {Type: "text", NotNull: boolPtr(true)},
		}
		messages := `[ { "key": "test1", "value": { "a": "NULL" }, "headers": {"operation": "insert"} } ]`
		result, err := ParseMessages([]byte(messages), schema)
		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.True(t, IsChangeMessage(result[0]))
		assert.Equal(t, "NULL", result[0].Change.Value["a"])

		messagesNullStringAndReal := `[ { "key": "test1", "value": { "a": "{\"a\",\"NULL\",NULL,\"b\"}" }, "headers": {"operation": "insert"} } ]`

		schema = Schema{
			"a": {Type: "text", Dims: intPtr(1)},
		}
		result, err = ParseMessages([]byte(messagesNullStringAndReal), schema)
		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.True(t, IsChangeMessage(result[0]))
		expected := []interface{}{"a", "NULL", nil, "b"}
		assert.Equal(t, expected, result[0].Change.Value["a"])

		// should fail if the array contains null in a non null type
		schema = Schema{
			"a": {Type: "text", Dims: intPtr(1), NotNull: boolPtr(true)},
		}
		_, err = ParseMessages([]byte(messagesNullStringAndReal), schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), `Column "a" does not allow NULL values`)
	})

	t.Run("should parse arrays including null values", func(t *testing.T) {
		schema := Schema{
			"a": {Type: "int2", Dims: intPtr(1)},
		}

		messages := `[ { "key": "test1", "value": { "a": "{1,2,NULL,4,5}" }, "headers": {"operation": "insert"} } ]`
		result, err := ParseMessages([]byte(messages), schema)
		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.True(t, IsChangeMessage(result[0]))
		expected := []interface{}{1, 2, nil, 4, 5}
		assert.Equal(t, expected, result[0].Change.Value["a"])
	})

	t.Run("should support custom parsers", func(t *testing.T) {
		// Create a custom parser that parses numeric as float64 instead of string
		customParser := Parser{
			"numeric": func(value string, additionalInfo ...*ColumnInfo) (Value, error) {
				return parseNumber(value, additionalInfo...)
			},
		}

		schema := Schema{
			"price": {Type: "numeric"},
		}

		messages := `[ { "key": "test1", "value": { "price": "123.45" }, "headers": {"operation": "insert"} } ]`
		result, err := ParseMessages([]byte(messages), schema, customParser)
		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.True(t, IsChangeMessage(result[0]))
		assert.Equal(t, 123.45, result[0].Change.Value["price"])
	})

	t.Run("should parse old_value with schema awareness", func(t *testing.T) {
		schema := Schema{
			"priority": {Type: "int4"},
			"active":   {Type: "bool"},
		}

		messages := `[
			{
				"key": "test1",
				"value": {"priority": "20", "active": "t"},
				"old_value": {"priority": "10", "active": "f"},
				"headers": {"operation": "update"}
			}
		]`

		result, err := ParseMessages([]byte(messages), schema)
		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.True(t, IsChangeMessage(result[0]))

		// Check that values are properly parsed according to schema
		assert.Equal(t, 20, result[0].Change.Value["priority"])
		assert.Equal(t, true, result[0].Change.Value["active"])
		assert.Equal(t, 10, result[0].Change.OldValue["priority"])
		assert.Equal(t, false, result[0].Change.OldValue["active"])
	})
}

// Test the original ParseMessages function for backwards compatibility
func TestParseMessages(t *testing.T) {
	schema := Schema{
		"id":       {Type: "uuid"},
		"title":    {Type: "text"},
		"priority": {Type: "int4"},
	}

	t.Run("ParseChangeMessages", func(t *testing.T) {
		jsonData := `[
			{
				"key": "test-id-1",
				"value": {"id": "test-id-1", "title": "Test Issue", "priority": 10},
				"headers": {"operation": "insert"}
			},
			{
				"key": "test-id-2", 
				"value": {"id": "test-id-2", "title": "Updated Issue", "priority": 20},
				"headers": {"operation": "update"}
			},
			{
				"key": "test-id-3",
				"value": {},
				"headers": {"operation": "delete"}
			}
		]`

		messages, err := ParseMessages([]byte(jsonData), schema)
		require.NoError(t, err)
		require.Len(t, messages, 3)

		// Test insert message
		assert.True(t, IsChangeMessage(messages[0]))
		assert.Equal(t, "test-id-1", messages[0].Change.Key)
		assert.Equal(t, OpInsert, messages[0].Change.Headers.Operation)
		assert.Equal(t, "test-id-1", messages[0].Change.Value["id"])
		assert.Equal(t, "Test Issue", messages[0].Change.Value["title"])
		assert.Equal(t, float64(10), messages[0].Change.Value["priority"]) // JSON numbers become float64

		// Test update message
		assert.True(t, IsChangeMessage(messages[1]))
		assert.Equal(t, "test-id-2", messages[1].Change.Key)
		assert.Equal(t, OpUpdate, messages[1].Change.Headers.Operation)

		// Test delete message
		assert.True(t, IsChangeMessage(messages[2]))
		assert.Equal(t, "test-id-3", messages[2].Change.Key)
		assert.Equal(t, OpDelete, messages[2].Change.Headers.Operation)
	})

	t.Run("ParseControlMessages", func(t *testing.T) {
		jsonData := `[
			{
				"headers": {"control": "up-to-date"}
			},
			{
				"headers": {"control": "must-refetch"}
			}
		]`

		messages, err := ParseMessages([]byte(jsonData), schema)
		require.NoError(t, err)
		require.Len(t, messages, 2)

		// Test up-to-date message
		assert.True(t, IsControlMessage(messages[0]))
		assert.True(t, IsUpToDateMessage(messages[0]))
		assert.Equal(t, "up-to-date", messages[0].Control.Headers.Control)

		// Test must-refetch message
		assert.True(t, IsControlMessage(messages[1]))
		assert.False(t, IsUpToDateMessage(messages[1]))
		assert.Equal(t, "must-refetch", messages[1].Control.Headers.Control)
	})

	t.Run("ParseMixedMessages", func(t *testing.T) {
		jsonData := `[
			{
				"key": "test-id-1",
				"value": {"id": "test-id-1", "title": "Test Issue"},
				"headers": {"operation": "insert"}
			},
			{
				"headers": {"control": "up-to-date"}
			}
		]`

		messages, err := ParseMessages([]byte(jsonData), schema)
		require.NoError(t, err)
		require.Len(t, messages, 2)

		assert.True(t, IsChangeMessage(messages[0]))
		assert.True(t, IsControlMessage(messages[1]))
		assert.True(t, IsUpToDateMessage(messages[1]))
	})

	t.Run("ParseEmptyArray", func(t *testing.T) {
		jsonData := `[]`

		messages, err := ParseMessages([]byte(jsonData), schema)
		require.NoError(t, err)
		assert.Len(t, messages, 0)
	})

	t.Run("ParseInvalidJSON", func(t *testing.T) {
		jsonData := `invalid json`

		_, err := ParseMessages([]byte(jsonData), schema)
		assert.Error(t, err)
	})

	t.Run("ParseNilSchema", func(t *testing.T) {
		jsonData := `[
			{
				"key": "test-id-1",
				"value": {"id": "test-id-1", "title": "Test Issue"},
				"headers": {"operation": "insert"}
			}
		]`

		messages, err := ParseMessages([]byte(jsonData), nil)
		require.NoError(t, err)
		require.Len(t, messages, 1)

		assert.True(t, IsChangeMessage(messages[0]))
		assert.Equal(t, "test-id-1", messages[0].Change.Key)
	})

	t.Run("ParseMessageWithOldValue", func(t *testing.T) {
		jsonData := `[
			{
				"key": "test-id-1",
				"value": {"id": "test-id-1", "title": "Updated Title", "priority": 20},
				"old_value": {"title": "Old Title", "priority": 10},
				"headers": {"operation": "update"}
			}
		]`

		messages, err := ParseMessages([]byte(jsonData), schema)
		require.NoError(t, err)
		require.Len(t, messages, 1)

		assert.True(t, IsChangeMessage(messages[0]))
		assert.Equal(t, OpUpdate, messages[0].Change.Headers.Operation)
		assert.Equal(t, "Updated Title", messages[0].Change.Value["title"])
		assert.Equal(t, "Old Title", messages[0].Change.OldValue["title"])
		assert.Equal(t, float64(10), messages[0].Change.OldValue["priority"])
	})

	t.Run("ParseControlMessageWithLSN", func(t *testing.T) {
		jsonData := `[
			{
				"headers": {
					"control": "up-to-date",
					"global_last_seen_lsn": "16/B374D848"
				}
			}
		]`

		messages, err := ParseMessages([]byte(jsonData), schema)
		require.NoError(t, err)
		require.Len(t, messages, 1)

		assert.True(t, IsControlMessage(messages[0]))
		assert.True(t, IsUpToDateMessage(messages[0]))
		assert.Equal(t, "up-to-date", messages[0].Control.Headers.Control)
		assert.Equal(t, "16/B374D848", messages[0].Control.Headers.GlobalLastSeenLSN)
	})
}

// Helper functions
func intPtr(i int) *int {
	return &i
}

func boolPtr(b bool) *bool {
	return &b
}

func mustParseBigInt(s string) *big.Int {
	num := new(big.Int)
	num.SetString(s, 10)
	return num
}
