package goclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHelpers(t *testing.T) {
	// Create test messages similar to TypeScript tests
	changeMsg := Message{
		Change: &ChangeMessage{
			Key:   "key",
			Value: Row{"key": "value"},
			Headers: struct {
				Operation Operation `json:"operation"`
			}{Operation: OpInsert},
		},
	}

	upToDateMsg := Message{
		Control: &ControlMessage{
			Headers: struct {
				Control           string `json:"control"`
				GlobalLastSeenLSN string `json:"global_last_seen_lsn,omitempty"`
			}{Control: "up-to-date"},
		},
	}

	mustRefetchMsg := Message{
		Control: &ControlMessage{
			Headers: struct {
				Control           string `json:"control"`
				GlobalLastSeenLSN string `json:"global_last_seen_lsn,omitempty"`
			}{Control: "must-refetch"},
		},
	}

	t.Run("CorrectlyDetectChangeMessages", func(t *testing.T) {
		assert.True(t, IsChangeMessage(changeMsg))
		assert.False(t, IsControlMessage(changeMsg))
	})

	t.Run("CorrectlyDetectControlMessages", func(t *testing.T) {
		assert.True(t, IsControlMessage(upToDateMsg))
		assert.True(t, IsControlMessage(mustRefetchMsg))
		assert.False(t, IsChangeMessage(upToDateMsg))
		assert.False(t, IsChangeMessage(mustRefetchMsg))
	})

	t.Run("CorrectlyDetectUpToDateMessage", func(t *testing.T) {
		assert.True(t, IsUpToDateMessage(upToDateMsg))
		assert.False(t, IsUpToDateMessage(mustRefetchMsg))
		assert.False(t, IsUpToDateMessage(changeMsg))
	})
}

func TestValidateParams(t *testing.T) {
	t.Run("ValidParams", func(t *testing.T) {
		params := map[string]string{
			"table":   "users",
			"where":   "active = true",
			"columns": "id,name,email",
		}
		err := ValidateParamKeys(params)
		assert.NoError(t, err)
	})

	t.Run("ReservedParams", func(t *testing.T) {
		params := map[string]string{
			"table":  "users",
			"live":   "true",  // Reserved
			"offset": "123_0", // Reserved
		}
		err := ValidateParamKeys(params)
		assert.Error(t, err)
		assert.IsType(t, ReservedParamError{}, err)

		reservedErr := err.(ReservedParamError)
		assert.Contains(t, reservedErr.Keys, "live")
		assert.Contains(t, reservedErr.Keys, "offset")
	})

	t.Run("NilParams", func(t *testing.T) {
		err := ValidateParams(nil)
		assert.NoError(t, err)
	})

	t.Run("EmptyParams", func(t *testing.T) {
		params := map[string]string{}
		err := ValidateParamKeys(params)
		assert.NoError(t, err)
	})
}
