package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type TaskStatus int

const (
	TaskStatusIdle TaskStatus = iota
	TaskStatusInProgress
	TaskStatusCompleted
)

type TaskDetail struct {
	Task      Task
	Status    TaskStatus
	StartTime time.Time
}

type Coordinator struct {
	MapTasks    []TaskDetail
	ReduceTasks []TaskDetail

	NReduce int
	// Reduce should start after all Map tasks are completed
	CurrentPhase TaskType
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NReduce:      nReduce,
		CurrentPhase: TaskTypeMap,
		MapTasks:     make([]TaskDetail, len(files)),
		ReduceTasks:  make([]TaskDetail, nReduce),
	}

	for i, filename := range files {
		c.MapTasks[i] = TaskDetail{
			Task: Task{
				Type:     TaskTypeMap,
				Filename: filename,
				TaskNum:  i,
				NReduce:  nReduce,
			},
			Status: TaskStatusIdle,
		}
	}

	c.server()
	return &c
}
