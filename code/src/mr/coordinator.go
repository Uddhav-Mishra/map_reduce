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
	// Your definitions here.
	map_status map[string]int
	map_stage_start_time map[string]int64
	map_file_to_id map[string]int
	map_stage_completed_status bool
    
    cur_map_job_counter int
    nreduce int

    reduce_status map[int]int
    reduce_stage_start_time map[int]int64
    reduce_stage_completed_status bool
}

// to protect coordinator variables in struct
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
		c.map_status[arg.INPUT_FILE] = 2
		delete(c.map_stage_start_time, arg.INPUT_FILE)
	} else if arg.IS_REDUCE {
		c.reduce_status[arg.REDUCE_ID] = 2
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
	// Also add check to remove from pending tasks if taking more than 10secs
	// update map stage completed status
	c.map_stage_completed_status = true
	for _,val := range c.map_status {
		if val != 2 {
			c.map_stage_completed_status = false
			break
		}
	}

	// update reduce stage completed status
	c.reduce_stage_completed_status = true
	for _, status := range c.reduce_status {
		if status != 2 {
			c.reduce_stage_completed_status = false
			break
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
	c.cur_map_job_counter = 0
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
	go c.Periodic()
	return &c
}
