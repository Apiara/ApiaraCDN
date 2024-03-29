package dominique

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
)

const (
	QueryKeyParam              = "key"
	QueryKeyTypeParam          = "by"
	QueryAccumulationTypeParam = "function"
	QueryIncTimestepParam      = "timestep"
	QueryStartTimeParam        = "start"
	QueryEndTimeParam          = "end"
)

// Creates internal dataAccessQuery from API query request
func createSessionDataQuery(urlQuery url.Values, timeseries TimeseriesDBReader) (dataAccessQuery, error) {
	var err error
	var query dataAccessQuery

	// Retrieve search range and lookup key
	query.start, err = time.Parse(QueryTimeFormat, urlQuery.Get(QueryStartTimeParam))
	if err != nil {
		return query, err
	}
	query.end, err = time.Parse(QueryTimeFormat, urlQuery.Get(QueryEndTimeParam))
	if err != nil {
		return query, err
	}
	query.key = urlQuery.Get(QueryKeyParam)

	// Retrieve query search mechanism
	switch urlQuery.Get(QueryKeyTypeParam) {
	case UIDSearchKey:
		query.lookup = timeseries.ReadEndpointSessions
	case CIDSearchKey:
		query.lookup = timeseries.ReadContentSessions
	default:
		return query, fmt.Errorf("got invalid search 'by' parameter: %s", urlQuery.Get(QueryKeyTypeParam))
	}

	// Set query type
	queryType := urlQuery.Get(QueryAccumulationTypeParam)
	if queryType == IncrementQuery {
		query.timestep, err = time.ParseDuration(urlQuery.Get(QueryIncTimestepParam))
		if err != nil {
			return query, nil
		}
	} else if queryType != SumQuery {
		return query, fmt.Errorf("invalid query type %s", queryType)
	}

	query.queryType = queryType
	return query, nil
}

// StartDataAccessAPI starts the API used to query for CDN usage data
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
	log.Fatal(http.ListenAndServe(listenAddr, accessMux))
}
