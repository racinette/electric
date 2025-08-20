package goclient

const (
	// HTTP Headers
	LiveCacheBusterHeader = "electric-cursor"
	ShapeHandleHeader     = "electric-handle"
	ChunkLastOffsetHeader = "electric-offset"
	ShapeSchemaHeader     = "electric-schema"
	ChunkUpToDateHeader   = "electric-up-to-date"

	// Query Parameters
	ColumnsQueryParam        = "columns"
	LiveCacheBusterParam     = "cursor"
	ShapeHandleQueryParam    = "handle"
	LiveQueryParam           = "live"
	OffsetQueryParam         = "offset"
	TableQueryParam          = "table"
	WhereQueryParam          = "where"
	ReplicaParam             = "replica"
	WhereParamsParam         = "params"
	ExperimentalLiveSSEParam = "experimental_live_sse"

	// Control flow constants
	ForceDisconnectAndRefresh = "force-disconnect-and-refresh"
	PauseStream               = "pause-stream"
)
