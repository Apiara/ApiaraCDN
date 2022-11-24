package dominique

import (
  "time"
)

type TimeseriesDB interface {
  WriteReport(t time.Time, r Report) error
  WriteDescription(t time.Time, desc SessionDescription) error
}

type InfluxTimeseriesDB struct {

}
