package infrastructure

type StatusResponse struct {
  Status ProcessingStatus `json:"Status"`
  FunctionalID *string `json:"FunctionalID"`
}
