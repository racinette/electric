package goclient

import (
	"encoding/json"
)

// Value represents a JSON value coming from the shape stream.
// It covers primitive JSON types plus nested arrays/objects.
type Value interface{}

// Row is a map of column -> parsed value
type Row map[string]Value

// Offset format: "-1" or "<lsn>_<chunkIndex>"
type Offset string

// Operation is the row change kind
type Operation string

const (
	OpInsert Operation = "insert"
	OpUpdate Operation = "update"
	OpDelete Operation = "delete"
)

// Header holds non-control/operation headers
type Header map[string]Value

// ControlMessage indicates control flow like up-to-date/must-refetch
type ControlMessage struct {
	Headers struct {
		Control           string `json:"control"`
		GlobalLastSeenLSN string `json:"global_last_seen_lsn,omitempty"`
		// Additional headers unmarshaled via RawHeaders
	} `json:"headers"`
}

// ChangeMessage represents an insert/update/delete
type ChangeMessage struct {
	Key      string `json:"key"`
	Value    Row    `json:"value"`
	OldValue Row    `json:"old_value,omitempty"`
	Headers  struct {
		Operation Operation `json:"operation"`
	} `json:"headers"`
}

// Message is either a Change or Control message
type Message struct {
	Change  *ChangeMessage
	Control *ControlMessage
}

func (m Message) IsChange() bool  { return m.Change != nil }
func (m Message) IsControl() bool { return m.Control != nil }

// Schema describes column types. Minimal subset needed for parsing.
type ColumnInfo struct {
	Type    string `json:"type"`
	Dims    *int   `json:"dims,omitempty"`
	NotNull *bool  `json:"not_null,omitempty"`
	// Additional metadata for specific types is captured as raw JSON
	Raw json.RawMessage `json:"-"`
}

type Schema map[string]ColumnInfo
