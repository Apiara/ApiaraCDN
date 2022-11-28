package infrastructure

const (
	ContentIDHeader    = "content_id"
	FunctionalIDHeader = "functional_id"
	ByteSizeHeader     = "bytes"
)

type ProcessingStatus string

const (
	RunningProcessing  ProcessingStatus = "running"
	FailedProcessing   ProcessingStatus = "failed"
	FinishedProcessing ProcessingStatus = "finished"
)

const (
	AESKeyStorageDir    = "/aes/key/"
	CryptDataStorageDir = "/cryptdata/"
	PartialMapDir       = "/mediamap/partial/"
	CompleteMediaMapDir = "/mediamap/complete/"
)
