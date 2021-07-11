package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"

import "os"
import "io/ioutil"
import "strconv"
import "sort"
import "encoding/json"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff) % nreduce_
}

// Map id of current running task. When we request job from coordinator map id is provided
// which we use to store the output file name.
var worker_map_id_ int;
// The reduce job id, When running the job we read all input
var nreduce_ int;
// Max map id that we have to consider while running reduce job. Input files to be used
// for any reduce jobs are for x  in (0, map_count_) : mr-map-x-reduceid
var map_count_ int;
// prefix for file paths that we are storing for map and reduce output.
var file_path_ string


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	path, err := os.Getwd()
	if err != nil {
            log.Fatal(err)
    }
    file_path_ = path + "/"
	// uncomment to send the Example RPC to the coordinator.
	worker_map_id_ = 0
	nreduce_ = 1
	CallRequestJob(mapf, reducef)
}


func CallRequestJob(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
    arg := RequestJobArg{}
    ret := RequestJobRet{}

    resp := call("Coordinator.RequestJob", &arg, &ret)
    if resp == false {
 		fmt.Println("RPC to coordinator failed , Retrying");
 		// Check how to avoid stack overflow here and other places
    	// where nested calls are being made.
 		CallRequestJob(mapf, reducef)
    	return
    }

    // RPC call completed, do map/reduce/exit/wait
    nreduce_ = ret.NREDUCE
    map_count_ = ret.MAP_COUNT
    if ret.IS_MAP {
    	//fmt.Println("map now" + ret.INPUT_FILE)
    	worker_map_id_ = ret.MAP_ID
    	StartMap(ret.INPUT_FILE, mapf, reducef)
    } else if ret.IS_REDUCE {
    	//fmt.Println("reduce")
      	StartReduce(ret.REDUCE_ID, mapf, reducef)
    } else if ret.WORK_DONE {
    	fmt.Println("All work done, exiting worker")
    } else {
    	//fmt.Println("chill for 2 secs and then retry for requesting job")
    	time.Sleep(2000 * time.Millisecond);
    	// TODO : fix possible stackoverflow
    	CallRequestJob(mapf, reducef)
    }
}


func StartMap(input_file string, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// read content from input file
	file, err := os.Open(input_file)
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", input_file)
	}
	file.Close()
	kva := mapf(input_file, string(content))

	// open/create mr-(worker_map_id_)-reduce(0 to nreduce-1)
	var write_files map[int]*os.File
	write_files = make(map[int]*os.File)
	for i := 0; i < nreduce_; i = i + 1 {
		name := file_path_
		//fmt.Println(name)
		ofile, _ := ioutil.TempFile(name, "mr-temp-"+strconv.Itoa(worker_map_id_)+"-"+strconv.Itoa(i))
		write_files[i] = ofile
	}
	//fmt.Println(len(kva))
	sort.Sort(ByKey(kva))
	
    for _, kv := range kva {
    	//enc := json.NewEncoder(write_files[ihash(kv.Key)])
    	//err := enc.Encode(&kv)
    	json_out,_ := json.MarshalIndent(kv, "", "")
    	write_files[ihash(kv.Key)].Write(json_out)
    	if err != nil {
    		log.Fatalf("unable to encode to json key =  %v %v", kv.Key, kv.Value)
    	}
	}



	// Map is completed
	complete_map_arg := CompleteJobArg{}
	complete_map_arg.IS_MAP = true
	complete_map_arg.INPUT_FILE = input_file

	complete_map_ret := CompleteJobRet{}

	// Now that all writes are complete. rename temp files to expected names
	// for map output files.
	for i := 0; i < nreduce_; i = i + 1 {

		write_files[i].Close()
		name := file_path_+"mr-map-"+strconv.Itoa(worker_map_id_)+"-"+strconv.Itoa(i)
		os.Rename(write_files[i].Name(), name)
	}

	complete_rpc := call("Coordinator.CompleteJob", &complete_map_arg, &complete_map_ret)
	if !complete_rpc {
		log.Fatalf("Unable to send complete rpc to coordinator")
	}

	// TODO : fix possible stackoverflow
    CallRequestJob(mapf, reducef)
}

func StartReduce(reduce_id int, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for i := 0; i < map_count_; i += 1 {
		filename := file_path_+"mr-map-" + strconv.Itoa(i)+"-"+strconv.Itoa(reduce_id)
		file, err := os.Open(filename)
		dec := json.NewDecoder(file)
		if err != nil {
			fmt.Println("Error : cannot read %v", filename)
			continue
		}
  		for {
			var kv KeyValue
    		if err := dec.Decode(&kv); err != nil {
     	 		break
    		}	
  			intermediate = append(intermediate, kv)
  		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(reduce_id)
	ofile, _ := os.Create(oname)

	//
	// Call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-reduce_id.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	// Given Reduce is completed, let the coordinator know.
	// If a crash happens before this RPC call is received on coordinator, current task will be
	// rescheduled and the output files generated on new worker will replace the files written
	// by this one, since we are not sure if the current worker completed job or not.
	complete_map_arg := CompleteJobArg{}
	complete_map_arg.IS_REDUCE = true
	complete_map_arg.REDUCE_ID = reduce_id

	complete_map_ret := CompleteJobRet{}
	complete_rpc := call("Coordinator.CompleteJob", &complete_map_arg, &complete_map_ret)
	if !complete_rpc {
		log.Fatalf("Unable to send complete rpc to coordinator")
	}
	// Current job done, Check if stack overflow needs to be handled here.
    CallRequestJob(mapf, reducef)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
