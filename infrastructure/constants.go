package infrastructure

// Standard query names for service API usage
const (
	RegionServerIDParam    = "server_id"
	ServerPublicAddrParam  = "server_public"
	ServerPrivateAddrParam = "server_private"

	ContentIDParam           = "content_id"
	ContentFunctionalIDParam = "functional_id"
	ContentByteSizeParam     = "bytes"

	MMDBFileNameParam = "mmdb"

	ContentRuleParam = "content_rule"
)

// Query names for services in Debugging/Testing mode
const (
	DebugModeForcedRequestIPParam = "debugging_ip"
)

// Status type for a cyprus data processing job
type ProcessingStatus string

const (
	RunningProcessing  ProcessingStatus = "running"
	FailedProcessing   ProcessingStatus = "failed"
	FinishedProcessing ProcessingStatus = "finished"
)

// Directory subpaths for cyprus data storage
const (
	AESKeyStorageDir    = "/aes/key/"
	CryptDataStorageDir = "/cryptdata/"
	PartialMapDir       = "/mediamap/partial/"
	CompleteMediaMapDir = "/mediamap/complete/"
)
