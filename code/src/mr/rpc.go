package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type RequestJobArg struct {
}

type RequestJobRet struct {
	// If map task is alloted to worker.
	IS_MAP bool
	// Input file on which map needs to be done.
	INPUT_FILE string
	MAP_ID int

	IS_REDUCE bool
	REDUCE_ID int

	NREDUCE int
	MAP_COUNT int
	
	WORK_DONE bool

	CHILL bool
}

type CompleteJobArg struct {
	IS_MAP bool
	INPUT_FILE string

	IS_REDUCE bool
	REDUCE_ID int
}

type CompleteJobRet struct {

}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
