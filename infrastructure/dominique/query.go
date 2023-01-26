package dominique

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

var (
	// Expected time format for data API requests
	QueryTimeFormat = time.RFC3339

	// Search key parameter for queries
	UIDSearchKey = "userid"
	CIDSearchKey = "contentid"

	// Query output format parameter
	SumQuery       = "sum"
	IncrementQuery = "inc"
)

// query response with summary of bytes served within StartTime and EndTime range
type queryResponse struct {
	StartTime           time.Time `json:"start_time"`
	EndTime             time.Time `json:"end_time"`
	DomesticBytesServed int64     `json:"domestic"`
	ForeginBytesServed  int64     `json:"foreign"`
}

/*
query response containing multiple queryResponses, usually consisting of queryResponses
with time ranges that together create a single larger continuous time range
*/
type multiQueryResponse struct {
	Datapoints []queryResponse `json:"datapoints"`
}

// dataAccessQuery is a structure used to perform a timeseries query on a key
type dataAccessQuery struct {
	lookup    func(string, time.Time, time.Time) ([]SessionDescription, error)
	key       string
	start     time.Time
	end       time.Time
	queryType string
	timestep  time.Duration
}

// Runs a query that sums all the data in the 'start' and 'end' time range
func (q *dataAccessQuery) runSumQuery() (interface{}, error) {
	// Perform lookup
	datapoints, err := q.lookup(q.key, q.start, q.end)
	if err != nil {
		return nil, err
	}

	// Perform sum
	domestic := int64(0)
	foreign := int64(0)
	for _, description := range datapoints {
		domestic += description.BytesRecv
		foreign += description.BytesNeeded - description.BytesRecv
	}

	// Return JSON marshalable response
	return queryResponse{q.start, q.end, domestic, foreign}, nil
}

// Runs a query that returns the result in time slices of duration 'timestep'
func (q *dataAccessQuery) runTimestepQuery() (interface{}, error) {
	// Create segmented response structure
	response := multiQueryResponse{make([]queryResponse, 0)}

	// Create intial time range for first sub query
	start := q.start
	end := start.Add(q.timestep)
	if end.After(q.end) {
		end = q.end
	}
	for start != q.end {
		// Create and perform sub query
		subQuery := dataAccessQuery{
			lookup:    q.lookup,
			key:       q.key,
			start:     start,
			end:       end,
			queryType: SumQuery,
		}
		subResponse, err := subQuery.runSumQuery()
		if err != nil {
			return nil, err
		}

		// Store query result
		response.Datapoints = append(response.Datapoints, subResponse.(queryResponse))

		// Update time slice range
		start = end
		end = start.Add(q.timestep)
		if end.After(q.end) {
			end = q.end
		}
	}

	// Return JSON marshalable response
	return response, nil
}

/*
Run runs 'runSumQuery' or 'runTimestepQuery' based on 'queryType'
then performs response marshaling
*/
func (q *dataAccessQuery) run() (io.Reader, error) {
	var err error
	var result interface{}

	// Identify and perform correct query mechanism
	switch q.queryType {
	case SumQuery:
		result, err = q.runSumQuery()
	case IncrementQuery:
		result, err = q.runTimestepQuery()
	default:
		err = fmt.Errorf("invalid query type %s", q.queryType)
	}

	if err != nil {
		return nil, err
	}

	// Marshal query and return
	body, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return bytes.NewBuffer(body), nil
}
