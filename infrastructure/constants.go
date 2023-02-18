package infrastructure

const (
	ServerIDHeader = "server_id"

	ContentIDHeader           = "content_id"
	ContentFunctionalIDHeader = "functional_id"
	ByteSizeHeader            = "bytes"

	RegionNameHeader   = "region_id"
	LocationHeader     = "location"
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
