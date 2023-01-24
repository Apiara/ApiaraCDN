package dominique

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
)

var (
	// Expected time format for data API requests
	TimeFormat = time.RFC3339

	// Search key parameter for queries
	UIDSearchKey = "userid"
	CIDSearchKey = "contentid"

	// Query output format parameter
	SumQuery       = "sum"
	IncrementQuery = "inc"
)

// dataAccessQuery is a structure used to perform a timeseries query on a key
type dataAccessQuery struct {
	lookup    func(string, time.Time, time.Time) ([]SessionDescription, error)
	key       string
	start     time.Time
	end       time.Time
	queryType string
	timestep  time.Duration
}

type queryResponse struct {
	Time                time.Time `json:"time"`
	DomesticBytesServed int64     `json:"domestic"`
	ForeginBytesServed  int64     `json:"foreign"`
}

type multiQueryResponse struct {
	Datapoints []queryResponse `json:"datapoints"`
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
	return queryResponse{q.start, domestic, foreign}, nil
}

// Runs a query that returns increments of length 'timestep' between 'start' and 'end'
func (q *dataAccessQuery) runTimestepQuery(timestep time.Duration) (interface{}, error) {
	// Perform sub lookups and sub sum queries
	totalIncrements := int(q.end.Sub(q.start) / timestep)
	response := multiQueryResponse{make([]queryResponse, 0)}
	start := q.start
	end := start.Add(timestep)
	for i := 0; i < totalIncrements; i++ {
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
		response.Datapoints = append(response.Datapoints, subResponse.(queryResponse))

		start = end
		end = start.Add(timestep)
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

	switch q.queryType {
	case SumQuery:
		result, err = q.runSumQuery()
	case IncrementQuery:
		result, err = q.runTimestepQuery(q.timestep)
	default:
		err = fmt.Errorf("invalid query type %s", q.queryType)
	}

	if err != nil {
		return nil, err
	}

	body, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return bytes.NewBuffer(body), nil
}

func createSessionDataQuery(urlQuery url.Values, timeseries TimeseriesDBReader) (dataAccessQuery, error) {
	// Retrieve search range and key
	var err error
	var query dataAccessQuery
	query.start, err = time.Parse(TimeFormat, urlQuery.Get("start"))
	if err != nil {
		return query, err
	}
	query.end, err = time.Parse(TimeFormat, urlQuery.Get("end"))
	if err != nil {
		return query, err
	}
	query.key = urlQuery.Get("key")

	// Retrieve query search mechanism
	switch urlQuery.Get("by") {
	case UIDSearchKey:
		query.lookup = timeseries.ReadEndpointSessions
	case CIDSearchKey:
		query.lookup = timeseries.ReadContentSessions
	default:
		return query, fmt.Errorf("got invalid search 'by' parameter: %s", urlQuery.Get("by"))
	}

	// Set query type
	queryType := urlQuery.Get("function")
	if queryType == IncrementQuery {
		query.timestep, err = time.ParseDuration(urlQuery.Get("timestep"))
		if err != nil {
			return query, nil
		}
	} else if queryType != SumQuery {
		return query, fmt.Errorf("invalid query type %s", queryType)
	}

	query.queryType = queryType
	return query, nil
}

func StartDataAccessAPI(listenAddr string, timeseries TimeseriesDBReader) {

	accessMux := http.NewServeMux()
	accessMux.HandleFunc(infra.DominiqueDataAPIFetchResource,
		func(resp http.ResponseWriter, req *http.Request) {
			// Retrieve query range
			queryValues := req.URL.Query()
			query, err := createSessionDataQuery(queryValues, timeseries)
			if err != nil {
				log.Println(err)
				resp.WriteHeader(http.StatusInternalServerError)
				return
			}

			// Perform Query
			body, err := query.run()
			if err != nil {
				log.Println(err)
				resp.WriteHeader(http.StatusInternalServerError)
				return
			}

			// Write query response
			if _, err = io.Copy(resp, body); err != nil {
				log.Println(err)
				resp.WriteHeader(http.StatusInternalServerError)
				return
			}
		})
}
