package mr

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const ExpirationDuration = time.Second * 10

type Coordinator struct {
	// Your definitions here.
	files              []string
	nReduce            int
	availableFileIndex []int
	monitorTask        map[string]time.Time
	mux                sync.Mutex

	reduceFiles              [][]string
	availableReduceFileIndex []int
	ticker                   *time.Ticker
	done                     bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AcquireTask(_ *AcquireTaskArgs, reply *AcquireTaskReply) error {
	if len(c.availableFileIndex) > 0 {
		err := func() error {
			c.mux.Lock()
			defer c.mux.Unlock()
			if len(c.availableFileIndex) > 0 {
				taskIdx := c.availableFileIndex[len(c.availableFileIndex)-1]
				c.availableFileIndex = c.availableFileIndex[:len(c.availableFileIndex)-1]
				reply.TaskName = TaskNameMap
				reply.TaskFiles = []string{c.files[taskIdx]}

				taskID := TaskNameIdx2TaskID(reply.TaskName, taskIdx)
				reply.TaskID = taskID
				c.monitorTask[taskID] = time.Now()
				return nil
			}
			return fmt.Errorf("error empty")
		}()
		if err == nil {
			return nil
		}
	}
	if len(c.availableFileIndex) == 0 {
		c.mux.Lock()
		defer c.mux.Unlock()
		if len(c.availableFileIndex) == 0 && len(c.monitorTask) == 0 && len(c.availableReduceFileIndex) > 0 {
			taskIdx := c.availableReduceFileIndex[len(c.availableReduceFileIndex)-1]
			c.availableReduceFileIndex = c.availableReduceFileIndex[:len(c.availableReduceFileIndex)-1]
			reply.TaskName = TaskNameReduce
			reply.TaskFiles = c.reduceFiles[taskIdx]

			taskID := TaskNameIdx2TaskID(reply.TaskName, taskIdx)
			reply.TaskID = taskID
			c.monitorTask[taskID] = time.Now()
			return nil
		}
	}
	return fmt.Errorf("error empty")
}

func TaskNameIdx2TaskID(name TaskName, id int) string {
	return fmt.Sprintf("%v:%v", name, id)
}

func TaskID2TaskNameIdx(id string) (TaskName, int) {
	res := strings.Split(id, ":")
	tID, _ := strconv.Atoi(res[1])
	return TaskName(res[0]), tID
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	if _, exists := c.monitorTask[args.TaskID]; exists {
		switch args.TaskName {
		case TaskNameMap:
			for idx, files := range args.DestFiles {
				if files != nil {
					c.reduceFiles[idx] = append(c.reduceFiles[idx], files...)
				}
			}
			delete(c.monitorTask, args.TaskID)
		case TaskNameReduce:
			delete(c.monitorTask, args.TaskID)
		}
	}
	return nil
}

func (c *Coordinator) monitor() {
loop:
	for {
		select {
		case <-c.ticker.C:
			if c.done {
				c.ticker.Stop()
				break loop
			}

			func() {
				c.mux.Lock()
				defer c.mux.Unlock()
				if len(c.availableReduceFileIndex) == 0 && len(c.availableFileIndex) == 0 && len(c.monitorTask) == 0 {
					c.done = true
					return
				}

				now := time.Now()
				for k, v := range c.monitorTask {
					if v.Add(ExpirationDuration).Before(now) {
						taskName, id := TaskID2TaskNameIdx(k)
						switch taskName {
						case TaskNameMap:
							c.availableFileIndex[len(c.availableFileIndex)] = id
							fmt.Printf("Task[%v] %v failed", taskName, k)
						case TaskNameReduce:
							c.availableReduceFileIndex[len(c.availableReduceFileIndex)] = id
							fmt.Printf("Task[%v] %v failed", taskName, k)
						}

					}
				}
			}()
		}
	}
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	availableFileIndex := make([]int, len(files), len(files))
	for i := 0; i < len(files); i++ {
		availableFileIndex[i] = i
	}

	c := Coordinator{
		files:                    files,
		nReduce:                  nReduce,
		availableFileIndex:       availableFileIndex,
		availableReduceFileIndex: make([]int, 0),
		reduceFiles:              make([][]string, nReduce),
		ticker:                   time.NewTicker(time.Second),
		done:                     false,
	}

	// Your code here.
	c.server()
	return &c
}
