package infrastructure

const (
	ContentIDHeader    = "content_id"
	FunctionalIDHeader = "functional_id"
	ByteSizeHeader     = "bytes"
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
