package goclient

import (
	"fmt"
)

type FetchError struct {
	Status  int
	Text    string
	JSON    map[string]any
	Headers map[string]string
	URL     string
}

func (e *FetchError) Error() string {
	if e.Text != "" {
		return fmt.Sprintf("HTTP Error %d at %s: %s", e.Status, e.URL, e.Text)
	}
	if e.JSON != nil {
		return fmt.Sprintf("HTTP Error %d at %s: %v", e.Status, e.URL, e.JSON)
	}
	return fmt.Sprintf("HTTP Error %d at %s", e.Status, e.URL)
}

type FetchBackoffAbortError struct{}

func (e FetchBackoffAbortError) Error() string {
	return "Fetch operation was aborted during backoff"
}

type MissingShapeURLError struct{}

func (e MissingShapeURLError) Error() string {
	return "Invalid shape options: missing required url parameter"
}

type MissingShapeHandleError struct{}

func (e MissingShapeHandleError) Error() string {
	return "shapeHandle is required if this isn't an initial fetch (i.e. offset > -1)"
}

type InvalidSignalError struct{}

func (e InvalidSignalError) Error() string {
	return "Signal must be a valid context.Context"
}

type ReservedParamError struct{ Keys []string }

func (e ReservedParamError) Error() string {
	return fmt.Sprintf("Cannot use reserved Electric parameter names in custom params: %s", e.Keys)
}

type MissingHeadersError struct {
	URL            string
	MissingHeaders []string
}

func (e MissingHeadersError) Error() string {
	msg := fmt.Sprintf("The response for the shape request to %s didn't include the following required headers:\n", e.URL)
	for _, h := range e.MissingHeaders {
		msg += "- " + h + "\n"
	}
	msg += "\nThis is often due to a proxy not setting CORS correctly so that all Electric headers can be read by the client."
	msg += "\nFor more information visit the troubleshooting guide: /docs/guides/troubleshooting/missing-headers"
	return msg
}

type ParserNullValueError struct {
	ColumnName string
}

func (e ParserNullValueError) Error() string {
	return fmt.Sprintf("Column \"%s\" does not allow NULL values", e.ColumnName)
}
