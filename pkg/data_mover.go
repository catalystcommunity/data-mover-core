package pkg

import (
	"errors"
	"time"

	"github.com/catalystcommunity/app-utils-go/errorutils"
	"github.com/catalystcommunity/app-utils-go/logging"
	"github.com/catalystcommunity/app-utils-go/parallelism"
	"github.com/sirupsen/logrus"
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
	// The source dispatcher sets sourceLoadComplete to true when no data is returned from the source connector, which
	// stops the source loading loop.
	sourceLoadComplete bool
	// when run is false both dispatchers will stop and Move() will exit. Dispatchers set this to false when either the
	// source or destination error handler returns false, which indicates that we should stop loading and exit
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
	// sourceDispatcher sets sourceLoadComplete to true when it gets an empty result set back, or run to false when
	// the error handler returns false, so run while there is data to load and the error handler hasn't indicated
	// that we should stop
	for !d.sourceLoadComplete && d.run {
		// submit job to source dispatcher. Source dispatcher submits jobs to destination dispatcher to persist data
		d.sourceDispatcher.Submit(SourceJob{})
	}
	// source loading complete, wait for dispatchers to complete
	d.sourceDispatcher.Wait()
	d.destinationDispatcher.Wait()
	d.end = time.Now()
	if !d.run {
		// d.run is set to false when the error handler returns false, which
		// triggers a stop of the mover. return an error to the caller to
		// indicate that the move was stopped early
		err = errors.New("error during move")
	}
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
		logging.Log.Error("source count does not match destination count")
	}
	return moveStats
}
