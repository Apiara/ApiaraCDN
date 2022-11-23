package cyprus

import (
  "fmt"
  "sync"
  infra "github.com/Apiara/ApiaraCDN/infrastructure"
)

type job struct {
  status infra.ProcessingStatus
  result string
}

/* jobTracker is used to by the service API keep track of the status of
processing jobs */
type jobTracker struct {
  jobs map[string]*job
  mutex *sync.RWMutex
}

func newJobTracker() *jobTracker {
  return &jobTracker{
    jobs: make(map[string]*job),
    mutex: &sync.RWMutex{},
  }
}

func (j *jobTracker) newJob(id string) {
  j.mutex.Lock()
  j.jobs[id] = &job{
    status: infra.RunningProcessing,
    result: "",
  }
  j.mutex.Unlock()
}

func (j *jobTracker) updateStatus(id string, status infra.ProcessingStatus) error {
  j.mutex.Lock()
  defer j.mutex.Unlock()

  if foundJob, ok := j.jobs[id]; ok {
    foundJob.status = status
    return nil
  }
  return fmt.Errorf("No job with id %s", id)
}

func (j *jobTracker) updateResult(id string, result string) error {
  j.mutex.Lock()
  defer j.mutex.Unlock()

  if foundJob, ok := j.jobs[id]; ok {
    foundJob.result = result
    return nil
  }
  return fmt.Errorf("No job with id %s", id)
}

func (j *jobTracker) free(id string) {
  j.mutex.Lock()
  delete(j.jobs, id)
  j.mutex.Unlock()
}

func (j *jobTracker) status(id string) (infra.ProcessingStatus, error) {
  j.mutex.RLock()
  defer j.mutex.RUnlock()

  if job, ok := j.jobs[id]; ok {
    return job.status, nil
  }
  return "", fmt.Errorf("No job with ID %s", id)
}

func (j *jobTracker) result(id string) (string, error) {
  j.mutex.RLock()
  defer j.mutex.RUnlock()

  if job, ok := j.jobs[id]; ok {
    return job.result, nil
  }
  return "", fmt.Errorf("No job with ID %s", id)
}
