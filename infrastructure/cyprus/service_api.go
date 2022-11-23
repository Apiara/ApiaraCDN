package cyprus

import (
  "log"
  "net/http"
  "encoding/json"
  infra "github.com/Apiara/ApiaraCDN/infrastructure"
)

/* handleProcessRequest goes through the preprocess, process, publish
workflow while keeping track of progress and results via the jobTracker */
func handleProcessRequest(cid string, preprocessor DataPreprocessor,
  processor DataProcessor, storage StorageManager, tracker *jobTracker) {

  tracker.newJob(cid)
  ingest, err := preprocessor.IngestMedia(cid)
  if err != nil {
    log.Println(err)
    tracker.updateStatus(cid, infra.FailedProcessing)
    return
  }

  digest, err := processor.DigestMedia(ingest)
  if err != nil {
    log.Println(err)
    tracker.updateStatus(cid, infra.FailedProcessing)
    return
  }
  tracker.updateResult(cid, digest.FunctionalID)

  if err = storage.Publish(digest); err != nil {
    log.Println(err)
    tracker.updateStatus(cid, infra.FailedProcessing)
    return
  }
  tracker.updateStatus(cid, infra.FinishedProcessing)
}

/* StartDataProcessingAPI starts the API used for processing and
publishing data for use on the network */
func StartDataProcessingAPI(listenAddr string, preprocessor DataPreprocessor,
  processor DataProcessor, storage StorageManager) {
  tracker := newJobTracker()
  processingAPI := http.NewServeMux()

  // Start a new processing job
  processingAPI.HandleFunc("/process", func(resp http.ResponseWriter, req *http.Request) {
    cid := req.URL.Query().Get(infra.ContentIDHeader)
    go handleProcessRequest(cid, preprocessor, processor, storage, tracker)
  })

  // Check status of a processing job and retrieve results
  processingAPI.HandleFunc("/status", func(resp http.ResponseWriter, req *http.Request) {
    cid := req.URL.Query().Get(infra.ContentIDHeader)
    status, err := tracker.status(cid)
    if err != nil {
      log.Println(err)
      resp.WriteHeader(http.StatusInternalServerError)
      return
    }

    response := infra.StatusResponse{Status: status}
    switch status {
    case infra.FinishedProcessing:
      fid, err := tracker.result(cid)
      if err != nil {
        log.Println(err)
        resp.WriteHeader(http.StatusInternalServerError)
        return
      }
      response.FunctionalID = &fid
      tracker.free(cid)
      break
    case infra.FailedProcessing:
      tracker.free(cid)
      break
    }

    if err = json.NewEncoder(resp).Encode(&response); err != nil {
      log.Println(err)
      resp.WriteHeader(http.StatusInternalServerError)
    }
  })

  // Delete published resources associated with passed in content id
  processingAPI.HandleFunc("/delete", func(resp http.ResponseWriter, req *http.Request) {
    cid := req.URL.Query().Get(infra.ContentIDHeader)
    if err := storage.PurgeByURL(cid); err != nil {
      log.Println(err)
      resp.WriteHeader(http.StatusInternalServerError)
    }
  })

  log.Fatal(http.ListenAndServe(listenAddr, processingAPI))
}
