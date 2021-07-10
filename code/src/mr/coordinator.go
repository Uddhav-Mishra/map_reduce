package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
//import "fmt"
import "sync"
//import "strconv"

type Coordinator struct {
	// Input file to status of map task.
	// status = 0 : yet to map, 1: map running, 2: map complete
	map_status map[string]int
	// Input file to start time of map process.
	map_stage_start_time map[string]int64
	// Input file to ID (1 to file_count)
	map_file_to_id map[string]int
	// Update periodically from values of map_status, If any map is 
	map_stage_completed_status bool
    
    // Total number of reduce jobs.
    nreduce int

    // Reduce job id to status of reduce task. 
    // status = 0 : yet to map, 1: map running, 2: map complete
    reduce_status map[int]int
    // Reduce id to start time of reduce task.
    reduce_stage_start_time map[int]int64
    // Uses 'reduce_status' to determine if reduce stage is completed
    // or not. 
    reduce_stage_completed_status bool
}

// To protect coordinator variables in struct.
// A granular lock can be used for protecting different variables.
var mu sync.Mutex

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestJob(arg *RequestJobArg, ret *RequestJobRet) error {
	//fmt.Println("Received rpc request job")
	mu.Lock()
	ret.CHILL = true
	ret.NREDUCE = c.nreduce 
	ret.MAP_COUNT = len(c.map_status)
	if c.map_stage_completed_status == false {
		for key,val := range c.map_status {
			if val == 0 {
				ret.CHILL = false
				ret.IS_MAP = true
				ret.INPUT_FILE = key
				ret.MAP_ID = c.map_file_to_id[key]
				c.map_status[key] = 1
				c.map_stage_start_time[key] = time.Now().UnixNano() / 1000000000
				//fmt.Println("sending map for "+ strconv.Itoa(ret.MAP_ID))
				//fmt.Println(c.map_stage_start_time[key])
				break
			}
		}
	} else if c.reduce_stage_completed_status == false {
		for index, status := range c.reduce_status {
			if status == 0 {
				ret.CHILL = false
				ret.IS_REDUCE = true
				ret.REDUCE_ID = index
				//fmt.Println("sending reduce for "+strconv.Itoa(index))
				c.reduce_status[index] = 1
				c.reduce_stage_start_time[index] = time.Now().UnixNano() / 1000000000
				//fmt.Println(c.reduce_stage_start_time[index])
				break;
			}
		}
	} else {
		ret.WORK_DONE = true;
		ret.CHILL = false;
	}
	mu.Unlock()
	return nil
}

func (c *Coordinator) CompleteJob(arg *CompleteJobArg, ret *CompleteJobRet) error {

	mu.Lock()
	if arg.IS_MAP {
		//fmt.Println("got map completed for " + strconv.Itoa(c.map_file_to_id[arg.INPUT_FILE]))
		c.map_status[arg.INPUT_FILE] = 2
		delete(c.map_stage_start_time, arg.INPUT_FILE)
	} else if arg.IS_REDUCE {
		c.reduce_status[arg.REDUCE_ID] = 2
		//fmt.Println("got reduce completed for " + strconv.Itoa(arg.REDUCE_ID))
		delete(c.reduce_stage_start_time, arg.REDUCE_ID)
	}
	mu.Unlock()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	//ret := false
	// Your code here.
	mu.Lock()
	overall := c.map_stage_completed_status && c.reduce_stage_completed_status
	mu.Unlock()
	return overall
}

func (c *Coordinator) Periodic() {
	//fmt.Println("Starting periodic checks")

	mu.Lock()
	// Checks to remove from pending tasks if taking more than 10secs
	// update map stage completed status
	cur_time := time.Now().UnixNano() / 1000000000
	var max_time_diff_allowed int64
	max_time_diff_allowed = 10 // 10 minutes in secs

	c.map_stage_completed_status = true
	for key,val := range c.map_status {
		if val != 2 {
			c.map_stage_completed_status = false
		}
		if val == 1 {
			x := (cur_time - c.map_stage_start_time[key])
			//fmt.Println("Running from " + strconv.FormatInt(x, 10))
			if (x > max_time_diff_allowed) {
				c.map_status[key] = 0
				delete(c.map_stage_start_time, key)
			}	
		}
	}

	// update reduce stage completed status
	c.reduce_stage_completed_status = true
	for key, status := range c.reduce_status {
		if status != 2 {
			c.reduce_stage_completed_status = false
		}
		if status == 1 && (cur_time - c.reduce_stage_start_time[key] >  max_time_diff_allowed) {
			c.reduce_status[key] = 0
			delete(c.reduce_stage_start_time, key)
		}
	}
	//fmt.Println("Marking map stage as " + strconv.FormatBool(c.map_stage_completed_status))
	//fmt.Println("Marking reduce stage as " + strconv.FormatBool(c.reduce_stage_completed_status))

	mu.Unlock()

	// 10s periodic event
	time.Sleep(10000 * time.Millisecond)
	c.Periodic()
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	mu.Lock()
	c.map_status = make(map[string]int)
	c.map_file_to_id = make(map[string]int)
	c.map_stage_start_time = make(map[string]int64)
	c.reduce_stage_start_time = make(map[int]int64)
	c.reduce_status = make(map[int]int)
	c.nreduce = nReduce
	//fmt.Println(nReduce)

	c.map_stage_completed_status = false
	c.reduce_stage_completed_status = false

	// Your code here.
	i := 0
	for _, x := range files {
		c.map_file_to_id[x] = i
		c.map_status[x] = 0
		i += 1
	}

	for i := 0; i < nReduce; i = i + 1 {
		c.reduce_status[i] = 0
	}
	mu.Unlock()

	c.server()
	// Start the periodic process in separate thread.
	// Overall coordinator uses locks and is thread-safe.
	go c.Periodic()
	return &c
}
