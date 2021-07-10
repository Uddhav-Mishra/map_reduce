# map_reduce
Distributed map reduce implementation framework which provides support to spawn coordinator and multiple worker process to complete a given map reduce task.

## Implementation:
All workers contact the coordinator via RPC. Currently the implementation uses coordinator and worker on same machine(rpc changes and global shared file system needed to incorporate cross machine communication). 
- Word Count : When running wc.so the final result will contain count of all words seen in the input files.
- Indexer : When running indexer.so the final result will contain word , count of word, and files in which word is seen.

## Steps to run.
In code/src/main/
- Coordinator : go build -race -buildmode=plugin ../mrapps/wc.go; go run -race mrcoordinator.go pg*.txt
- Worker : go build -race -buildmode=plugin ../mrapps/wc.go; go run -race mrworker.go wc.so
- Intermediate map files are stored as mr-map-mapId-reduceId, and final output files are stored as mr-out-reduceId
- Run test : bash test-mr.sh , Multiple runs bash test-mr-many.sh 10

## RPC
- RequestJob : Worker makes this RPC to coordinator to get map/reduce task and process the received task. 
- CompleteJob : Once a map/reduce task is completed on a worker, this rpc is sent to coordinator to make sure that the current task can be marked as completed by coordinator.

## Features:
- Parallelism : All workers can execute different map/reduce tasks and this helps us to get the task completed in parallel.
- Crash Resistant : Assuming worker can crash at any instant during the task execution. Avoid Partially written files : Uses temp file to write while processing map tasks. The temp files are renamed once the map task is completed.
- Job Reschedule : In case of delayed executions, Periodic process at coordinator will mark tasks to be rescheduled in case its not completed in specified time. 

## Enhancements that can be added to current implementation:
- Async RPC's : RPC calls are sync currently, making them async makes it efficient from CPU side.
- Stack Overflow : Possibility of stack overflow in worker while requesting for new job to process.(CallRequestJob)
- Granular locks : A granular lock can be introduced instead of one complete lock for protecting all map and reduce coordinator state variables.
- Gflags : Use gflags to define configurable constants like prefix of file name used in worker, no. of reduce tasks, periodic function time delay etc.
- Improve test-mr.sh for early exit test : wait -n does not work properly on mac to wait for any completed process. Currently waiting for one of the started worker processes instead of any of the worker process to finish.
- Coordinator crashes : Dealing with coordinator crashes in a better way.


