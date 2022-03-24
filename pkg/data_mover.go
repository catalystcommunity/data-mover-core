package pkg

import (
	"github.com/catalystsquad/app-utils-go/errorutils"
	"github.com/catalystsquad/app-utils-go/logging"
	"github.com/catalystsquad/app-utils-go/parallelism"
	"github.com/sirupsen/logrus"
	"time"
)

type dataMover struct {
	continueOnErr                           bool
	source                                  Source
	destination                             Destination
	sourceCount, destinationCount           *uint64
	start, end                              time.Time
	sourceErrors, destinationErrors         []error
	destinationDispatcher, sourceDispatcher *parallelism.Dispatcher
	run                                     bool
}

type stats struct {
	Duration                        time.Duration
	RecordsPerSecond                float64
	SourceErrors, DestinationErrors []error
	SourceCount, DestinationCount   uint64
}

func NewDataMover(sourceParallelism, destinationParallelism int, continueOnErr bool, source Source, destination Destination) (*dataMover, error) {
	sourceCount := uint64(0)
	destinationCount := uint64(0)

	// initialize data mover
	mover := &dataMover{
		continueOnErr:    continueOnErr,
		source:           source,
		destination:      destination,
		sourceCount:      &sourceCount,
		destinationCount: &destinationCount,
		run:              true,
	}

	// set dispatchers
	mover.sourceDispatcher = parallelism.NewDispatcher(sourceParallelism, SourceHandler{dataMover: mover})
	mover.destinationDispatcher = parallelism.NewDispatcher(destinationParallelism, DestinationHandler{dataMover: mover})

	// init source
	err := mover.source.Initialize()
	if err != nil {
		return mover, err
	}

	// init destination
	err = mover.destination.Initialize()

	// start dispatchers
	mover.sourceDispatcher.Start()
	mover.destinationDispatcher.Start()

	return mover, err
}

func (d *dataMover) Move() (moveStats stats, err error) {
	defer func() {
		recovered := recover()
		if err == nil && recovered != nil {
			err = errorutils.RecoverErr(recovered)
			errorutils.LogOnErr(nil, "error during move", err)
		}
	}()
	// start timer
	d.start = time.Now()
	// sourceDispatcher sets run to false when it gets an empty dataset back
	for d.run {
		// submit job to source dispatcher. Source dispatcher submits jobs to destination dispatcher to persist data
		d.sourceDispatcher.Submit(SourceJob{})
	}
	// source loading complete, wait for dispatchers to complete
	d.sourceDispatcher.Wait()
	d.destinationDispatcher.Wait()
	d.end = time.Now()
	moveStats = d.getStats()
	return
}

func (d *dataMover) getStats() stats {
	duration := d.end.Sub(d.start)
	moveStats := stats{
		Duration:          duration,
		RecordsPerSecond:  float64(*d.destinationCount) / duration.Seconds(),
		SourceErrors:      d.sourceErrors,
		DestinationErrors: d.destinationErrors,
		SourceCount:       *d.sourceCount,
		DestinationCount:  *d.destinationCount,
	}
	// log errors
	if len(moveStats.SourceErrors) > 0 || len(moveStats.DestinationErrors) > 0 {
		logging.Log.Error("Move completed with errors")
		for _, err := range moveStats.SourceErrors {
			errorutils.LogOnErr(nil, "error getting data from source", err)
		}
		for _, err := range moveStats.DestinationErrors {
			errorutils.LogOnErr(nil, "error persisting data to destination", err)
		}
	}
	// log moveStats
	logging.Log.WithFields(logrus.Fields{
		"duration":                moveStats.Duration.Seconds(),
		"records_per_second":      moveStats.RecordsPerSecond,
		"num_source_errors":       len(moveStats.SourceErrors),
		"num_destination_errors":  len(moveStats.DestinationErrors),
		"num_records_from_source": moveStats.SourceCount,
		"num_records_to_dest":     moveStats.DestinationCount,
	}).Info("Move complete")
	if moveStats.SourceCount != moveStats.DestinationCount {
		logging.Log.Error(nil, "source count does not match destination count")
	}
	return moveStats
}
