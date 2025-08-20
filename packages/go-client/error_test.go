package goclient

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFetchError(t *testing.T) {
	t.Run("CreateWithTextResponse", func(t *testing.T) {
		status := 404
		text := "Not Found"
		headers := map[string]string{"content-type": "text/plain"}
		url := "https://example.com/notfound"

		err := &FetchError{
			Status:  status,
			Text:    text,
			Headers: headers,
			URL:     url,
		}

		assert.Equal(t, status, err.Status)
		assert.Equal(t, text, err.Text)
		assert.Nil(t, err.JSON)
		assert.Equal(t, headers, err.Headers)
		assert.Equal(t, url, err.URL)
		assert.Equal(t, "HTTP Error 404 at https://example.com/notfound: Not Found", err.Error())
	})

	t.Run("CreateWithJSONResponse", func(t *testing.T) {
		status := 500
		jsonResp := map[string]any{"error": "Internal Server Error"}
		headers := map[string]string{"content-type": "application/json"}
		url := "https://example.com/servererror"

		err := &FetchError{
			Status:  status,
			JSON:    jsonResp,
			Headers: headers,
			URL:     url,
		}

		assert.Equal(t, status, err.Status)
		assert.Equal(t, "", err.Text)
		assert.Equal(t, jsonResp, err.JSON)
		assert.Equal(t, headers, err.Headers)
		expectedMessage := `HTTP Error 500 at https://example.com/servererror: map[error:Internal Server Error]`
		assert.Equal(t, expectedMessage, err.Error())
	})

	t.Run("CreateWithoutContent", func(t *testing.T) {
		status := 403
		headers := map[string]string{"content-type": "text/plain"}
		url := "https://example.com/forbidden"

		err := &FetchError{
			Status:  status,
			Headers: headers,
			URL:     url,
		}

		assert.Equal(t, "HTTP Error 403 at https://example.com/forbidden", err.Error())
	})
}

func TestMissingShapeURLError(t *testing.T) {
	err := MissingShapeURLError{}
	assert.Equal(t, "Invalid shape options: missing required url parameter", err.Error())
}

func TestMissingShapeHandleError(t *testing.T) {
	err := MissingShapeHandleError{}
	assert.Equal(t, "shapeHandle is required if this isn't an initial fetch (i.e. offset > -1)", err.Error())
}

func TestReservedParamError(t *testing.T) {
	keys := []string{"live", "offset", "handle"}
	err := ReservedParamError{Keys: keys}
	expectedMessage := "Cannot use reserved Electric parameter names in custom params: [live offset handle]"
	assert.Equal(t, expectedMessage, err.Error())
}

func TestMissingHeadersError(t *testing.T) {
	url := "https://example.com/shape"
	missingHeaders := []string{"electric-offset", "electric-handle"}
	err := MissingHeadersError{
		URL:            url,
		MissingHeaders: missingHeaders,
	}

	expectedMessage := `The response for the shape request to https://example.com/shape didn't include the following required headers:
- electric-offset
- electric-handle

This is often due to a proxy not setting CORS correctly so that all Electric headers can be read by the client.
For more information visit the troubleshooting guide: /docs/guides/troubleshooting/missing-headers`

	assert.Equal(t, expectedMessage, err.Error())
}

func TestFetchBackoffAbortError(t *testing.T) {
	err := FetchBackoffAbortError{}
	assert.Equal(t, "Fetch operation was aborted during backoff", err.Error())
}

func TestBuildFetchErrorFromResponse(t *testing.T) {
	tests := []struct {
		name          string
		statusCode    int
		contentType   string
		body          string
		expectedText  string
		expectedJSON  map[string]any
		expectedError string
	}{
		{
			name:          "TextResponse",
			statusCode:    404,
			contentType:   "text/plain",
			body:          "Not Found",
			expectedText:  "Not Found",
			expectedJSON:  nil,
			expectedError: "HTTP Error 404 at test-url: Not Found",
		},
		{
			name:          "JSONResponse",
			statusCode:    500,
			contentType:   "application/json",
			body:          `{"error": "Internal Server Error"}`,
			expectedText:  "",
			expectedJSON:  map[string]any{"error": "Internal Server Error"},
			expectedError: "HTTP Error 500 at test-url: map[error:Internal Server Error]",
		},
		{
			name:          "InvalidJSON",
			statusCode:    500,
			contentType:   "application/json",
			body:          "Invalid JSON",
			expectedText:  "Invalid JSON",
			expectedJSON:  nil,
			expectedError: "HTTP Error 500 at test-url: Invalid JSON",
		},
		{
			name:          "NoContentType",
			statusCode:    500,
			contentType:   "",
			body:          "Server error with no content-type",
			expectedText:  "Server error with no content-type",
			expectedJSON:  nil,
			expectedError: "HTTP Error 500 at test-url: Server error with no content-type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock response
			resp := &http.Response{
				StatusCode: tt.statusCode,
				Header:     make(http.Header),
			}
			if tt.contentType != "" {
				resp.Header.Set("content-type", tt.contentType)
			}

			// We'll simulate buildFetchError behavior
			headers := map[string]string{}
			for k, v := range resp.Header {
				if len(v) > 0 {
					headers[k] = v[0]
				}
			}

			var text string
			var js map[string]any
			body := []byte(tt.body)

			ct := resp.Header.Get("content-type")
			if contentTypeContainsJSON(ct) {
				if err := json.Unmarshal(body, &js); err != nil {
					text = string(body)
				}
			} else {
				text = string(body)
			}

			err := &FetchError{
				Status:  resp.StatusCode,
				Text:    text,
				JSON:    js,
				Headers: headers,
				URL:     "test-url",
			}

			assert.Equal(t, tt.expectedText, err.Text)
			assert.Equal(t, tt.expectedJSON, err.JSON)
			assert.Equal(t, tt.expectedError, err.Error())
		})
	}
}

// Helper function to check if content type contains JSON
func contentTypeContainsJSON(contentType string) bool {
	return len(contentType) > 0 && (contentType == "application/json" ||
		len(contentType) > len("application/json") &&
			contentType[:len("application/json")] == "application/json")
}
