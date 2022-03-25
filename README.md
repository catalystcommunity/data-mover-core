
# data-mover-core

The core of the Data Mover library. This has the interface definitions for Source and Destination connectors, as well as the core logic to instantiate and run a data mover.

## Usage
```go
source := SomeSourceType{} // instantiate a source that implements the Source interface
dest := SomeDestType{} // instantiate a destination that implements the Destination interface
// error handler interface, in this example this will be used for both source and destination errors
errorHandler := func(err error) bool {  
  fmt.Println(fmt.Sprintf("encountered error: %v", err))  
  return true  
}
// instantiate a new data mover
mover, err := pkg.NewDataMover(  
  10, // source parallelism
  10, // destination parallelism
  source, // source connector instance
  dest, // dest connector instance
  errorHandler, // source error handler
  errorHandler, // destination error handler
)
// call move, which will block until the move is complete, and return stats and an error
stats, err := mover.Move()
```

## Source Interface
The source interface has two methods

### Initialize
```go
Initialize() error
```

This is where you can do any initialization of your connector implementation. Connect to databases, create http clients, whatever you need to do. An error should be returned, when an error is returned by `Initialize` that error will be returned by `NewDataMover`

### GetData
```go
GetData() ([]map[string]interface{}, error)
```

This is the method that handles fetching data from the source. It should return an array of `map[string]interface{}` and an error. This is called in a loop until it returns an empty array. When `sourceParallelism` is greater than 1, this is called in parallel. Coordination of calls to `GetData` is a source implementation concern, so if there is any coordination or locking that must be done to be thread safe, or to page through data, that must be done in your `GetData` implementation. The mover makes no attempts to handle any of that. When an error is returned from `GetData` the `sourceErrorHandler` is called. If the `sourceErrorHandler` returns `false` then the `Move()` will stop and exit.

## Destination Interface

### Initialize

```go
Initialize() error
```

This is where you can do any initialization of your connector implementation. Connect to databases, create http clients, whatever you need to do. An error should be returned, when an error is returned by `Initialize` that error will be returned by `NewDataMover`

### Persist

```go
Persist(data []map[string]interface{}) error
```
Each iteration of the source's `GetData` will trigger a call to the destination's `Persist` method if any data is returned from the source. Your implementation should insert the data into the destination, returning an error. When an error is returned, the `destinationErrorHandler` is called. If the `destinationErrorHandler` returns `false` then the `Move()` will stop and exit.
