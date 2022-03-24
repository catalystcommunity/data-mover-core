package pkg

import (
	"github.com/catalystsquad/app-utils-go/errorutils"
	"github.com/catalystsquad/app-utils-go/parallelism"
	"sync/atomic"
)

type Source interface {
	Initialize() error
	GetData() ([]map[string]interface{}, error)
}

type SourceHandler struct {
	dataMover *dataMover
}

type SourceJob struct{}

func (h SourceHandler) HandleJob(job parallelism.Job) {
	data, err := h.dataMover.source.GetData()
	if err != nil {
		// add error to source errors
		h.dataMover.sourceErrors = append(h.dataMover.sourceErrors, err)
		if !h.dataMover.continueOnErr {
			// die
			errorutils.PanicOnErr(nil, "error getting source data", err)
		}
	}
	// if there's data, send it to the destination
	if len(data) > 0 {
		// got source data, increment source counter
		atomic.AddUint64(h.dataMover.sourceCount, uint64(len(data)))
		// submit data to destination
		h.dataMover.destinationDispatcher.Submit(DestinationJob{Data: data})
	} else {
		// no data returned, set run to false
		h.dataMover.run = false
	}
}

func (j SourceJob) GetData() interface{} {
	return nil
}
