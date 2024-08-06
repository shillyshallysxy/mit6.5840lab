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
type CompleteTaskArgs struct {
	TaskName  TaskName
	TaskFiles []string
	TaskID    string
	DestFiles [][]string
}

type AcquireTaskArgs struct {
}

type TaskName string

var (
	TaskNameMap    TaskName = "Map"
	TaskNameReduce TaskName = "Reduce"
)

type AcquireTaskReply struct {
	TaskName  TaskName
	TaskFiles []string
	TaskID    string

	NReduce int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
