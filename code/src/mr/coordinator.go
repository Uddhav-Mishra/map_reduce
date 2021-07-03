package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "fmt"

type Coordinator struct {
	// Your definitions here.
	map_status map[string]int
	map_stage_start_time map[string]int64
	map_stage_completed_status bool
    
    cur_map_job_counter int
    nreduce int

    reduce_status map[int]int
    reduce_stage_start_time map[int]int64
    reduce_stage_completed_status bool
}

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
	ret.CHILL = true
	ret.NREDUCE = c.nreduce 
	fmt.Println(c.nreduce)
	if c.map_stage_completed_status == false {
		for key,val := range c.map_status {
			if val == 0 {
				ret.CHILL = false
				ret.IS_MAP = true
				ret.INPUT_FILE = key
				c.map_status[key] = 1
				c.map_stage_start_time[key] = time.Now().UnixNano() / 1000000000
				fmt.Println(c.map_stage_start_time[key])
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
				fmt.Println(c.reduce_stage_start_time[index])
				break;
			}
		}
	} else {

	}
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
	return c.map_stage_completed_status && c.reduce_stage_completed_status
}

func (c *Coordinator) Periodic() {


	time.Sleep(1000 * time.Millisecond)
	go c.Periodic()
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.map_status = make(map[string]int)
	c.map_stage_start_time = make(map[string]int64)
	c.reduce_stage_start_time = make(map[int]int64)
	c.reduce_status = make(map[int]int)
	c.nreduce = nReduce
	fmt.Println(nReduce)
	c.cur_map_job_counter = 0
	c.map_stage_completed_status = false
	c.reduce_stage_completed_status = false

	// Your code here.
	for _, x := range files {
		c.map_status[x] = 0
	}

	for i := 0; i < nReduce; i = i + 1 {
		c.reduce_status[i] = 0
	}

	c.server()
	go c.Periodic()
	return &c
}
