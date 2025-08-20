package integration

import (
	"fmt"
	"reflect"
)

// isRealNumber checks if the value is a numeric type (excluding complex numbers).
func isRealNumber(v interface{}) bool {
	switch v.(type) {
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, uintptr,
		float32, float64:
		return true
	default:
		return false
	}
}

// toFloat64 converts a reflected numeric value to float64.
func toFloat64(v reflect.Value) (float64, error) {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(v.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return float64(v.Uint()), nil
	case reflect.Float32, reflect.Float64:
		return v.Float(), nil
	default:
		return 0, fmt.Errorf("non-numeric type")
	}
}

// toInt64 converts a reflected signed integer value to int64.
func toInt64(v reflect.Value) (int64, error) {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int(), nil
	default:
		return 0, fmt.Errorf("non-signed-integer type")
	}
}

// toUint64 converts a reflected unsigned integer value to uint64.
func toUint64(v reflect.Value) (uint64, error) {
	switch v.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint(), nil
	default:
		return 0, fmt.Errorf("non-unsigned-integer type")
	}
}

// CompareNumbers compares two numeric values of arbitrary types.
// Returns 0 if a == b, 1 if a > b, -1 if a < b, and an error for non-numeric types.
func CompareNumbers(a, b interface{}) (int, error) {
	if !isRealNumber(a) || !isRealNumber(b) {
		return 0, fmt.Errorf("non-numeric type")
	}

	va := reflect.ValueOf(a)
	vb := reflect.ValueOf(b)

	// Handle floating-point types by converting both to float64
	if va.Kind() == reflect.Float32 || va.Kind() == reflect.Float64 ||
		vb.Kind() == reflect.Float32 || vb.Kind() == reflect.Float64 {
		fa, err := toFloat64(va)
		if err != nil {
			return 0, err
		}
		fb, err := toFloat64(vb)
		if err != nil {
			return 0, err
		}
		if fa < fb {
			return -1, nil
		} else if fa > fb {
			return 1, nil
		}
		return 0, nil
	}

	// Handle signed integers
	if va.Kind() >= reflect.Int && va.Kind() <= reflect.Int64 &&
		vb.Kind() >= reflect.Int && vb.Kind() <= reflect.Int64 {
		ia, err := toInt64(va)
		if err != nil {
			return 0, err
		}
		ib, err := toInt64(vb)
		if err != nil {
			return 0, err
		}
		if ia < ib {
			return -1, nil
		} else if ia > ib {
			return 1, nil
		}
		return 0, nil
	}

	// Handle unsigned integers
	if va.Kind() >= reflect.Uint && va.Kind() <= reflect.Uintptr &&
		vb.Kind() >= reflect.Uint && vb.Kind() <= reflect.Uintptr {
		ua, err := toUint64(va)
		if err != nil {
			return 0, err
		}
		ub, err := toUint64(vb)
		if err != nil {
			return 0, err
		}
		if ua < ub {
			return -1, nil
		} else if ua > ub {
			return 1, nil
		}
		return 0, nil
	}

	// Handle mixed signed and unsigned integers
	var signedVal int64
	var unsignedVal uint64
	var signedIsA bool

	if va.Kind() >= reflect.Int && va.Kind() <= reflect.Int64 {
		signedVal, _ = toInt64(va)
		unsignedVal, _ = toUint64(vb)
		signedIsA = true
	} else {
		signedVal, _ = toInt64(vb)
		unsignedVal, _ = toUint64(va)
		signedIsA = false
	}

	if signedVal < 0 {
		if signedIsA {
			return -1, nil // a is negative, b is non-negative
		}
		return 1, nil // b is negative, a is non-negative
	}

	// Compare the non-negative signed value with the unsigned value
	if uint64(signedVal) < unsignedVal {
		if signedIsA {
			return -1, nil
		}
		return 1, nil
	} else if uint64(signedVal) > unsignedVal {
		if signedIsA {
			return 1, nil
		}
		return -1, nil
	}
	return 0, nil
}
