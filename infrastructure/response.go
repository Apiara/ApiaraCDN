package infrastructure

type StatusResponse struct {
	Status   ProcessingStatus        `json:"Status"`
	Metadata *PostProcessingMetadata `json:"metadata"`
}

type PostProcessingMetadata struct {
	FunctionalID string `json:"FunctionalID"`
	ByteSize     int64  `json:"bytes"`
}
