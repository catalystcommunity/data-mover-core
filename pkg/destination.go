package pkg

import (
	"github.com/catalystcommunity/app-utils-go/logging"
	"github.com/catalystcommunity/app-utils-go/parallelism"
	"sync/atomic"
)

// Destination is the interface that a destination connector must implement
type Destination interface {
	// Initialize can be used to initialize a source connector. I.E. db connections, http clients, etc.
	Initialize() error
	// Persist will be called for each batch of data returned by the source connector. This is where the logic should be
	// implemented to persist data from the source connector to the destination.
	Persist(data []map[string]interface{}) error
}

// Dispatcher implementations - These have nothing to do with connector interfaces and are used internally by the data mover.
// see https://github.com/catalystcommunity/app-utils-go/blob/main/parallelism/README.md for more info

// DestinationHandler is the data mover's destination dispatcher job handler
type DestinationHandler struct {
	dataMover *dataMover
}

// HandleJob is the data mover's destination dispatcher job handler implementation. This calls the configured destination
// connector's Persist() method with the data passed in on the job, from the source connector.
func (h DestinationHandler) HandleJob(job parallelism.Job) {
	// sourceDispatcher and destDispatcher set run to false when the error handler returns false, so run while the error
	// handler hasn't indicated that we should stop. This allows for graceful exit on error
	if h.dataMover.run {
		data := job.GetData().([]map[string]interface{})
		err := h.dataMover.destination.Persist(data)
		if err != nil {
			// call destination error handler
			keepRunning := h.dataMover.destErrorHandler(err)
			if !keepRunning {
				logging.Log.Error(nil, "exiting due to error in destination connector", err)
				h.dataMover.run = false
			}
		} else {
			// no error, increment destination count
			atomic.AddUint64(h.dataMover.destinationCount, uint64(len(data)))
		}
	}
}

// DestinationJob is the destination dispatcher's job struct, it has a field for the data from the source connector.
type DestinationJob struct {
	Data []map[string]interface{}
}

// GetData is the destination dispatcher's job GetData implementation, it returns the Data from the job.
func (j DestinationJob) GetData() interface{} {
	return j.Data
}
