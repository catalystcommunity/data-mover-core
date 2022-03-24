package pkg

import (
	"github.com/catalystsquad/app-utils-go/errorutils"
	"github.com/catalystsquad/app-utils-go/logging"
	"github.com/catalystsquad/app-utils-go/parallelism"
	"github.com/sirupsen/logrus"
	"time"
)

// dataMover is the data mover struct that holds configuration, references to the source and destination connectors
// and references to the source and destination dispatchers. Fields are intentionally unexported to make the use of
// the data mover simpler.
type dataMover struct {
	// the source connector
	source Source
	// the destination connector
	destination Destination
	// counts of records handled
	sourceCount, destinationCount *uint64
	// when the Move() started, and when it ended
	start, end time.Time
	// dispatchers for source and destination
	destinationDispatcher, sourceDispatcher *parallelism.Dispatcher
	// when run is false, Move() will exit. This is true when either there is no more data returned from the source
	// connector, or when either the source or destination error handler returns false
	run bool
	// error handlers. When an error occurs the error handler is called. The error handler should return a boolean value
	// indicating whether or not the Move() should continue. If the error handler returns true, the Move() keeps running.
	// if the error handler returns false, the Move() will stop and exit
	sourceErrorHandler, destErrorHandler func(error) bool
}

// stats are updated while the mover runs and returned by the Move() method.
type stats struct {
	Duration                        time.Duration
	RecordsPerSecond                float64
	SourceErrors, DestinationErrors []error
	SourceCount, DestinationCount   uint64
}

// NewDataMover is the constructor for a data mover. Use this to instantiate a new configured data mover.
func NewDataMover(sourceParallelism, destinationParallelism int, source Source, destination Destination, sourceErrorHandler, destErrorHandler func(error) bool) (*dataMover, error) {
	sourceCount := uint64(0)
	destinationCount := uint64(0)

	// initialize data mover
	mover := &dataMover{
		source:             source,
		destination:        destination,
		sourceCount:        &sourceCount,
		destinationCount:   &destinationCount,
		run:                true,
		sourceErrorHandler: sourceErrorHandler,
		destErrorHandler:   destErrorHandler,
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

// Move starts the move of data from source to destination. Move blocks until both the source and destination connectors
// have completed everything they need to do.
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

// getStats() retrieves stats for the Move()
func (d *dataMover) getStats() stats {
	duration := d.end.Sub(d.start)
	moveStats := stats{
		Duration:         duration,
		RecordsPerSecond: float64(*d.destinationCount) / duration.Seconds(),
		SourceCount:      *d.sourceCount,
		DestinationCount: *d.destinationCount,
	}
	// log moveStats
	logging.Log.WithFields(logrus.Fields{
		"duration":                moveStats.Duration.Seconds(),
		"records_per_second":      moveStats.RecordsPerSecond,
		"num_records_from_source": moveStats.SourceCount,
		"num_records_to_dest":     moveStats.DestinationCount,
	}).Info("Move complete")
	if moveStats.SourceCount != moveStats.DestinationCount {
		logging.Log.Error(nil, "source count does not match destination count")
	}
	return moveStats
}
