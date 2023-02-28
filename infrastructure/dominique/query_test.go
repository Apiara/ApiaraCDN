package dominique

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Creates test 'lookup' function that dataAccessQuery expects with predefined responses
func createTestLookup(responses map[time.Time][]SessionDescription) func(string, time.Time, time.Time) ([]SessionDescription, error) {
	return func(key string, start time.Time, end time.Time) ([]SessionDescription, error) {
		if ret, ok := responses[start]; ok {
			return ret, nil
		}
		return nil, fmt.Errorf("no values found")
	}
}

// Tests dataAccessQuery structures and methods
func TestDataAccessQuery(t *testing.T) {
	// Create resources
	timeIncrement := time.Second
	bytesRecv := int64(50)
	bytesNeeded := int64(100)
	totalTimes := 10
	responses := make(map[time.Time][]SessionDescription)
	start := time.Now()
	end := start
	for i := 0; i < totalTimes; i++ {
		responses[end] = []SessionDescription{
			{
				SessionID:   strconv.Itoa(i),
				BytesRecv:   bytesRecv,
				BytesNeeded: bytesNeeded,
			},
		}
		end = end.Add(timeIncrement)
	}

	// Test basic sum query
	basicQuery := dataAccessQuery{
		lookup:    createTestLookup(responses),
		key:       QueryKeyParam,
		start:     start,
		end:       end,
		queryType: SumQuery,
	}
	basicResponse, err := basicQuery.runSumQuery()
	assert.Nil(t, err, "Error should be nil")
	assert.Equal(t, bytesRecv, basicResponse.(queryResponse).DomesticBytesServed, "Wrong domestic bytes value")
	assert.Equal(t, bytesNeeded-bytesRecv, basicResponse.(queryResponse).ForeginBytesServed, "Wrong foreign bytes value")

	// Test increment query
	incQuery := dataAccessQuery{
		lookup:    createTestLookup(responses),
		key:       QueryKeyParam,
		start:     start,
		end:       end,
		queryType: IncrementQuery,
		timestep:  timeIncrement,
	}
	incResponse, err := incQuery.runTimestepQuery()
	assert.Nil(t, err, "Error should be nil")
	assert.Equal(t, totalTimes, len(incResponse.(multiQueryResponse).Datapoints))
}
