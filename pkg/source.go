package pkg

import (
	"github.com/catalystcommunity/app-utils-go/logging"
	"github.com/catalystcommunity/app-utils-go/parallelism"
	"sync/atomic"
)

// Source is the interface that a source connector must implement.
type Source interface {
	// Initialize can be used to initialize a source connector. I.E. db connections, http clients, etc.
	Initialize() error
	// GetData will be called until it returns an empty array. When sourceParallelism is greater than 1, GetData will
	// be called in parallel. The mover makes no attempt at tracking what data has been fetched already, nor does it
	// make any attempts at handling concurrency. That sort of tracking is 100% up to the implementation.
	GetData() ([]map[string]interface{}, error) // GetData
}

// Dispatcher implementations - These have nothing to do with connector interfaces and are used internally by the data mover.
// see https://github.com/catalystcommunity/app-utils-go/blob/main/parallelism/README.md for more info

// SourceHandler is the data mover's source dispatcher job handler
type SourceHandler struct {
	dataMover *dataMover
}

// HandleJob is the data mover's source dispatcher job handler implementation. This calls the configured source connector's
// GetData() function and submits the resulting data to the data mover's destination dispatcher to be handled by the
// destination connector
func (h SourceHandler) HandleJob(job parallelism.Job) {
	data, err := h.dataMover.source.GetData()
	if err != nil {
		// call destination error handler
		keepRunning := h.dataMover.sourceErrorHandler(err)
		if !keepRunning {
			logging.Log.Error(nil, "exiting due to error in source connector", err)
			h.dataMover.run = false
			return
		}
	}
	// if there's data, send it to the destination
	if len(data) > 0 {
		// got source data, increment source counter
		atomic.AddUint64(h.dataMover.sourceCount, uint64(len(data)))
		// submit data to destination
		h.dataMover.destinationDispatcher.Submit(DestinationJob{Data: data})
	} else {
		// no data returned, set sourceLoadComplete to true to stop the source loading loop
		h.dataMover.sourceLoadComplete = true
	}
}

// SourceJob has no fields because at this time we need no extra data, but it must be in place to implement the
// GetData interface
type SourceJob struct{}

// GetData returns nil because we need no job data at this time
func (j SourceJob) GetData() interface{} {
	return nil
}
