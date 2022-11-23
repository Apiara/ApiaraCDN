package infrastructure

const (
  ContentIDHeader = "content_id"
  FunctionalIDHeader = "functional_id"
)

type ProcessingStatus string
const (
  RunningProcessing ProcessingStatus = "running"
  FailedProcessing ProcessingStatus = "failed"
  FinishedProcessing ProcessingStatus = "finished"
)
