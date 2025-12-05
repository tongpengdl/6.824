package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
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
	// Protect shared access to the coordinator
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	completed_map_tasks := 0
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.CurrentPhase == TaskTypeMap {
		// hand over map tasks if any idle
		for i := range c.MapTasks {
			if c.MapTasks[i].Status == TaskStatusIdle {
				c.MapTasks[i].Status = TaskStatusInProgress
				c.MapTasks[i].StartTime = time.Now()
				reply.Task = c.MapTasks[i].Task
				return nil
			} else if c.MapTasks[i].Status == TaskStatusCompleted {
				completed_map_tasks++
			} else if c.MapTasks[i].Status == TaskStatusInProgress {
				// check for timeout and reassign
				if time.Since(c.MapTasks[i].StartTime) > 10*time.Second {
					log.Printf("Reassigning map task %d\n", c.MapTasks[i].Task.TaskNum)
					c.MapTasks[i].StartTime = time.Now()
					reply.Task = c.MapTasks[i].Task
					return nil
				}
			}
		}
		// Hand over wait task if no idle map tasks
		if completed_map_tasks < len(c.MapTasks) {
			reply.Task = Task{Type: TaskTypeWait}
			return nil
		}
		// Transition to reduce phase
		c.CurrentPhase = TaskTypeReduce
	}

	if c.CurrentPhase == TaskTypeReduce {
		completed_reduce_tasks := 0
		// hand over reduce tasks if any idle
		for i := range c.ReduceTasks {
			if c.ReduceTasks[i].Status == TaskStatusIdle {
				c.ReduceTasks[i].Status = TaskStatusInProgress
				c.ReduceTasks[i].StartTime = time.Now()
				reply.Task = c.ReduceTasks[i].Task
				return nil
			} else if c.ReduceTasks[i].Status == TaskStatusCompleted {
				completed_reduce_tasks++
			} else if c.ReduceTasks[i].Status == TaskStatusInProgress {
				// check for timeout and reassign
				if time.Since(c.ReduceTasks[i].StartTime) > 10*time.Second {
					log.Printf("Reassigning reduce task %d\n", c.ReduceTasks[i].Task.TaskNum)
					c.ReduceTasks[i].StartTime = time.Now()
					reply.Task = c.ReduceTasks[i].Task
					return nil
				}
			}
		}
		// Hand over wait task if no idle reduce tasks
		if completed_reduce_tasks < len(c.ReduceTasks) {
			reply.Task = Task{Type: TaskTypeWait}
			return nil
		}
		// All tasks are done
		reply.Task = Task{Type: TaskTypeExit}
		return nil
	}

	return nil

}

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

	// Prepare reduce tasks: each reduce worker will read mr-MapID-ReduceID
	// files for its ReduceID across all maps.
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = TaskDetail{
			Task: Task{
				Type:    TaskTypeReduce,
				TaskNum: i,
				// NMap lets the worker know how many map outputs to read.
				NMap: len(files),
				// Include NReduce for symmetry/possible worker use.
				NReduce: nReduce,
			},
			Status: TaskStatusIdle,
		}
	}

	c.server()
	return &c
}
