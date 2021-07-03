package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
// for uuid gen
import "os/exec"


import "os"
import "io/ioutil"
import "strconv"
import "sort"

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


var worker_uuid_ string;
var worker_map_id_ int;
var nreduce_ int;

func GetUuid() string {
	out, err := exec.Command("uuidgen").Output()
	if err != nil {
		fmt.Println("Unable to generate uuid")
		return GetUuid()
	}
	// check error and fatal
	return string(out)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	CallExample()
	fmt.Println("heree")
	worker_uuid_ := GetUuid()
	fmt.Println(worker_uuid_)
	worker_map_id_ = 1
	nreduce_ = 2
	CallRequestJob(true /*first_request*/, mapf, reducef)
}


func CallRequestJob(first_request bool, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
    arg := RequestJobArg{}
    arg.WORKER_UUID = worker_uuid_
    if first_request {
    	arg.FIRST_REQUEST = true
    }
    ret := RequestJobRet{}

    call("Coordinator.RequestJob", &arg, &ret)

    if ret.IS_MAP {
    	fmt.Println("map now" + ret.INPUT_FILE)
    	StartMap(ret.INPUT_FILE, mapf, reducef)
    } else if ret.IS_REDUCE {
    	fmt.Println("reduce")
      	StartReduce(ret.REDUCE_ID, mapf, reducef)
    } else {
    	fmt.Println("chill")
    	time.Sleep(2000 * time.Millisecond);
    	go CallRequestJob(false, mapf, reducef)
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
		name := "mr-output-"+strconv.Itoa(worker_map_id_)+"-"+strconv.Itoa(i)
		fmt.Println(name)
		ofile, _ := os.Create(name)
		write_files[i] = ofile
	}
	fmt.Println(len(kva))
	sort.Sort(ByKey(kva))
	for _,elem := range kva {
		fmt.Fprintf(write_files[ihash(elem.Key)], "%v %v\n", elem.Key, elem.Value)
	}

	for i := 0; i < nreduce_; i = i + 1 {
		write_files[i].Close()
	}

    CallRequestJob(false, mapf, reducef)
}

func StartReduce(reduce_id int, mapf func(string, string) []KeyValue, reducef func(string, []string) string, ) {

// go  CallRequestJob(false, mapf, reducef)
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
