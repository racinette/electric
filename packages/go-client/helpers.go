package goclient

import (
	"strings"
)

// IsChangeMessage checks if a message is a change message
func IsChangeMessage(msg Message) bool {
	return msg.Change != nil
}

// IsControlMessage checks if a message is a control message
func IsControlMessage(msg Message) bool {
	return msg.Control != nil
}

// IsUpToDateMessage checks if a message is an up-to-date control message
func IsUpToDateMessage(msg Message) bool {
	return IsControlMessage(msg) && strings.EqualFold(msg.Control.Headers.Control, "up-to-date")
}

// GetOffset extracts the offset from a message, returns empty string if not found
// The LSN is only present in the up-to-date control message when in SSE mode.
// If we are not in SSE mode this function will return an empty string.
func GetOffset(msg Message) Offset {
	if IsControlMessage(msg) && IsUpToDateMessage(msg) {
		if lsn := msg.Control.Headers.GlobalLastSeenLSN; lsn != "" {
			// In SSE mode, the up-to-date message may contain an offset
			// For now, we don't parse it since SSE is not implemented
			// This would need to be implemented when SSE support is added
		}
	}
	return ""
}

// IsMustRefetchMessage checks if a message is a must-refetch control message
func IsMustRefetchMessage(msg Message) bool {
	return IsControlMessage(msg) && strings.EqualFold(msg.Control.Headers.Control, "must-refetch")
}

// ValidateParams checks for reserved parameter usage
func ValidateParams(params *ShapeParams) error {
	if params == nil {
		return nil
	}
	return validateParamKeys(getParamKeys(params))
}

// ValidateParamKeys checks for reserved parameter usage in a string map
func ValidateParamKeys(params map[string]string) error {
	if len(params) == 0 {
		return nil
	}
	var keys []string
	for k := range params {
		keys = append(keys, k)
	}
	return validateParamKeys(keys)
}

// getParamKeys extracts keys from Params
func getParamKeys(params *ShapeParams) []string {
	keys := make([]string, 0, len(params.additional))
	if params.additional == nil {
		return keys
	}
	for k := range params.additional {
		keys = append(keys, k)
	}
	return keys
}

// validateParamKeys performs the actual validation
func validateParamKeys(keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	reserved := map[string]struct{}{
		LiveCacheBusterParam:  {},
		ShapeHandleQueryParam: {},
		LiveQueryParam:        {},
		OffsetQueryParam:      {},
	}

	var bad []string
	for _, k := range keys {
		if _, ok := reserved[k]; ok {
			bad = append(bad, k)
		}
	}

	if len(bad) > 0 {
		return ReservedParamError{Keys: bad}
	}

	return nil
}
