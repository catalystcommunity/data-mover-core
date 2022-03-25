package test

import (
	"encoding/json"
	"fmt"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/catalystsquad/data-mover-core/pkg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
)

var lock = new(sync.Mutex)
var numIterations, executedIterations int
var sourceData, destData []map[string]interface{}
var expectedRecords int64

// test suite definition and setup functions
type MoverSuite struct {
	suite.Suite
}

// empty for now, implement if needed
func (s *MoverSuite) SetupSuite() {}

// empty for now, implement if needed
func (s *MoverSuite) TearDownSuite() {}

func (s *MoverSuite) SetupTest() {
	// initialize vars before each test
	numIterations = gofakeit.Number(10000, 20000)
	expectedRecords, executedIterations = 0, 0
	sourceData, destData = []map[string]interface{}{}, []map[string]interface{}{}
}

// tests
func TestSuite(t *testing.T) {
	suite.Run(t, new(MoverSuite))
}

func (s *MoverSuite) TestConcurrentMove() {
	source := TestConcurrentMoveSource{}
	dest := TestConcurrentMoveDestination{}
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
	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), mover)
	stats, err := mover.Move()
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), expectedRecords, int64(len(sourceData)))
	assert.Equal(s.T(), expectedRecords, int64(len(destData)))
	assert.Equal(s.T(), getDataAsSortedString(s.T(), sourceData), getDataAsSortedString(s.T(), destData))
	assert.Equal(s.T(), uint64(len(sourceData)), stats.SourceCount)
	assert.Equal(s.T(), uint64(len(destData)), stats.DestinationCount)
	assert.Len(s.T(), stats.SourceErrors, 0)
	assert.Len(s.T(), stats.DestinationErrors, 0)
	assert.Greater(s.T(), stats.Duration.Microseconds(), int64(0)) // this is very fast because it's a unit tess.T(), milliseconds comes out as 0, so using microseconds
	assert.Greater(s.T(), stats.RecordsPerSecond, float64(0))
}

// helper functions
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

// source and destination implementations below
type TestConcurrentMoveSource struct{}

func (t TestConcurrentMoveSource) Initialize() error {
	return nil
}

func (t TestConcurrentMoveSource) GetData() ([]map[string]interface{}, error) {
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

type TestConcurrentMoveDestination struct{}

func (t TestConcurrentMoveDestination) Initialize() error {
	return nil
}

func (t TestConcurrentMoveDestination) Persist(data []map[string]interface{}) error {
	// append data to dest data in a thread safe manner
	appendDestData(data)
	return nil
}
