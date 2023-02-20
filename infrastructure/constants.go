package infrastructure

const (
	RegionServerIDHeader    = "server_id"
	ServerPublicAddrHeader  = "server_public"
	ServerPrivateAddrHeader = "server_private"

	ContentIDHeader           = "content_id"
	ContentFunctionalIDHeader = "functional_id"
	ByteSizeHeader            = "bytes"

	MMDBFileNameHeader = "mmdb"

	ContentRuleHeader = "content_rule"
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
