package pkg

import (
	"github.com/catalystsquad/app-utils-go/errorutils"
	"github.com/catalystsquad/app-utils-go/parallelism"
	"sync/atomic"
)

type Destination interface {
	Initialize() error
	Persist(data []map[string]interface{}) error
}

type DestinationHandler struct {
	dataMover *dataMover
}

type DestinationJob struct {
	Data []map[string]interface{}
}

func (h DestinationHandler) HandleJob(job parallelism.Job) {
	data := job.GetData().([]map[string]interface{})
	err := h.dataMover.destination.Persist(data)
	if err != nil {
		// add error to destination errors
		h.dataMover.destinationErrors = append(h.dataMover.destinationErrors, err)
		if !h.dataMover.continueOnErr {
			// die
			errorutils.PanicOnErr(nil, "error persisting data to destination", err)
		}
	} else {
		// no error, increment destination count
		atomic.AddUint64(h.dataMover.destinationCount, uint64(len(data)))
	}
}

func (j DestinationJob) GetData() interface{} {
	return j.Data
}
