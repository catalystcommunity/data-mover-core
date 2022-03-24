package test

import (
	"encoding/json"
	"fmt"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/catalystsquad/data-mover-core/pkg"
	"github.com/stretchr/testify/assert"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
)

var numIterations = gofakeit.Number(10000, 20000)
var executedIterations = 0
var sourceData []map[string]interface{}
var destData []map[string]interface{}
var lock = new(sync.Mutex)
var expectedRecords = int64(0)

type TestSource struct{}

func (t TestSource) Initialize() error {
	return nil
}

func (t TestSource) GetData() ([]map[string]interface{}, error) {
	data := []map[string]interface{}{}
	if executedIterations < numIterations {
		executedIterations++
		numRecords := gofakeit.Number(1, 3)
		for i := 0; i < numRecords; i++ {
			numKeys := gofakeit.Number(1, 3)
			record := map[string]interface{}{}
			for i := 0; i < numKeys; i++ {
				record[gofakeit.Name()] = gofakeit.HackerPhrase()
			}
			data = append(data, record)
			atomic.AddInt64(&expectedRecords, 1)
		}
	}
	// append generated data to source data in a thread safe manner
	appendSourceData(data)
	return data, nil
}

type TestDestination struct{}

func (t TestDestination) Initialize() error {
	return nil
}

func (t TestDestination) Persist(data []map[string]interface{}) error {
	// append data to dest data in a thread safe manner
	appendDestData(data)
	return nil
}

func TestConcurrentMove(t *testing.T) {
	source := TestSource{}
	dest := TestDestination{}
	errorHandler := func(err error) bool {
		fmt.Println(fmt.Sprintf("encountered error: %v", err))
		return true
	}
	mover, err := pkg.NewDataMover(
		10,
		10,
		source,
		dest,
		errorHandler,
		errorHandler,
	)
	assert.NoError(t, err)
	assert.NotNil(t, mover)
	stats, err := mover.Move()
	assert.NoError(t, err)
	assert.Equal(t, expectedRecords, int64(len(sourceData)))
	assert.Equal(t, expectedRecords, int64(len(destData)))
	assert.Equal(t, getDataAsSortedString(t, sourceData), getDataAsSortedString(t, destData))
	assert.Equal(t, uint64(len(sourceData)), stats.SourceCount)
	assert.Equal(t, uint64(len(destData)), stats.DestinationCount)
	assert.Len(t, stats.SourceErrors, 0)
	assert.Len(t, stats.DestinationErrors, 0)
	assert.Greater(t, stats.Duration.Microseconds(), int64(0)) // this is very fast because it's a unit test, milliseconds comes out as 0, so using microseconds
	assert.Greater(t, stats.RecordsPerSecond, float64(0))
}

func getDataAsSortedString(t *testing.T, data []map[string]interface{}) string {
	bytes, err := json.Marshal(data)
	assert.NoError(t, err)
	stringSlice := []rune(string(bytes))
	sort.Slice(stringSlice, func(i, j int) bool {
		return stringSlice[i] < stringSlice[j]
	})
	sortedString := string(stringSlice)
	return sortedString
}

func appendSourceData(data []map[string]interface{}) {
	lock.Lock()
	defer lock.Unlock()
	sourceData = append(sourceData, data...)
}

func appendDestData(data []map[string]interface{}) {
	lock.Lock()
	defer lock.Unlock()
	destData = append(destData, data...)
}
