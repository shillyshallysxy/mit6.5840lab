package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		reply := AcquireTaskReply{}
		// send the RPC request, wait for the reply.
		// the "Coordinator.Example" tells the
		// receiving server that we'd like to call
		// the Example() method of struct Coordinator.
		ok := call("Coordinator.AcquireTask", nil, &reply)
		if ok {
			fmt.Printf("reply.TaskName=%v, reply.TaskID=%v, reply.TaskFiles=%v\n", reply.TaskName, reply.TaskID, reply.TaskFiles)
			switch reply.TaskName {
			case TaskNameMap:
				kva := []KeyValue{}
				for _, filename := range reply.TaskFiles {
					file, err := os.Open(filename)
					if err != nil {
						log.Fatalf("cannot open %v", filename)
					}
					content, err := ioutil.ReadAll(file)
					if err != nil {
						log.Fatalf("cannot read %v", filename)
					}
					file.Close()
					kva = append(kva, mapf(filename, string(content))...)
				}

				partitionedVa := make(map[int][]KeyValue)
				for _, kv := range kva {
					hash := ihash(kv.Key) % reply.NReduce
					if _, exists := partitionedVa[hash]; !exists {
						partitionedVa[hash] = make([]KeyValue, 0)
					}
					partitionedVa[hash] = append(partitionedVa[hash], kv)
				}
				destFiles := make([][]string, 0, reply.NReduce)
				for k, v := range partitionedVa {
					filename := fmt.Sprintf("%v.%v.tmp", reply.TaskFiles[0], k)
					destFiles[k] = append(destFiles[k], filename)
					ofile, err := os.Open(filename)
					if err != nil {
						log.Fatalf("cannot open %v", filename)
					}
					for _, kv := range v {
						_, err = fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
						if err != nil {
							log.Fatalf("cannot write %v", filename)
						}
					}
					ofile.Close()
				}
				args := &CompleteTaskArgs{
					TaskID:    reply.TaskID,
					TaskName:  reply.TaskName,
					TaskFiles: reply.TaskFiles,
					DestFiles: destFiles,
				}
				ok := call("Coordinator.CompleteTask", args, nil)
				if ok {
					fmt.Println("Coordinator.CompleteTask Success")
				}

			case TaskNameReduce:
				intermediate := []KeyValue{}
				for _, filename := range os.Args[2:] {
					file, err := os.Open(filename)
					if err != nil {
						log.Fatalf("cannot open %v", filename)
					}
					content, err := ioutil.ReadAll(file)
					if err != nil {
						log.Fatalf("cannot read %v", filename)
					}
					file.Close()
					stra := strings.Split(string(content), "\n")

					for _, str := range stra {
						kv := strings.Split(str, " ")
						if len(kv) == 2 {
							intermediate = append(intermediate, KeyValue{Key: kv[0], Value: kv[1]})
						}
					}
				}

				sort.Sort(ByKey(intermediate))

				oname := reply.TaskID
				ofile, _ := os.Create(oname)

				//
				// call Reduce on each distinct key in intermediate[],
				// and print the result to mr-out-0.
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
				args := &CompleteTaskArgs{
					TaskID:    reply.TaskID,
					TaskName:  reply.TaskName,
					TaskFiles: reply.TaskFiles,
					DestFiles: nil,
				}
				ok := call("Coordinator.CompleteTask", args, nil)
				if ok {
					fmt.Println("Coordinator.CompleteTask Success")
				}

			}
		} else {
			fmt.Printf("call failed!\n")
		}
	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
