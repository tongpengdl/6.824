package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	for {
		task := GetTask()
		if task == nil {
			time.Sleep(time.Second)
			continue
		}

		switch task.Type {
		case TaskTypeMap:
			log.Printf("Starting Map Task %d on file %s\n", task.TaskNum, task.Filename)
			// Read input file
			content, err := os.ReadFile(task.Filename)
			if err != nil {
				log.Fatalf("cannot read %v", task.Filename)
			}
			// Call Map function
			kva := mapf(task.Filename, string(content))

			// Partition kva into nReduce intermediate files
			buckets := make([][]KeyValue, task.NReduce)
			for _, kv := range kva {
				reduceTaskNum := ihash(kv.Key) % task.NReduce
				buckets[reduceTaskNum] = append(buckets[reduceTaskNum], kv)
			}

			// Write intermediate files
			for i := 0; i < task.NReduce; i++ {
				intermediateFilename := fmt.Sprintf("mr-%d-%d", task.TaskNum, i)
				file, err := os.Create(intermediateFilename)
				if err != nil {
					log.Fatalf("cannot create %v", intermediateFilename)
				}
				enc := json.NewEncoder(file)
				for _, kv := range buckets[i] {
					if err := enc.Encode(&kv); err != nil {
						log.Fatalf("cannot encode kv pair %v", kv)
					}
				}
				file.Close()
			}
			log.Printf("Completed Map Task %d\n", task.TaskNum)
			ReportTaskCompletion(task)
		case TaskTypeReduce:
			log.Printf("Starting Reduce Task %d\n", task.TaskNum)
			// Read intermediate files
			intermediate := []KeyValue{}
			for i := 0; i < task.NMap; i++ {
				intermediateFilename := fmt.Sprintf("mr-%d-%d", i, task.TaskNum)
				file, err := os.Open(intermediateFilename)
				if err != nil {
					log.Fatalf("cannot open %v", intermediateFilename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						if err == io.EOF {
							break
						}
						log.Fatalf("decode error reading %v: %v", intermediateFilename, err)
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}

			// Sort intermediate key-value pairs by key
			sort.Slice(intermediate, func(i, j int) bool {
				return intermediate[i].Key < intermediate[j].Key
			})

			// Create output file
			outputFilename := fmt.Sprintf("mr-out-%d", task.TaskNum)
			outputFile, err := os.Create(outputFilename)
			if err != nil {
				log.Fatalf("cannot create %v", outputFilename)
			}

			// Apply Reduce function
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
				reducedValue := reducef(intermediate[i].Key, values)
				fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, reducedValue)
				i = j
			}
			outputFile.Close()
			log.Printf("Completed Reduce Task %d\n", task.TaskNum)
			ReportTaskCompletion(task)
		case TaskTypeWait:
			time.Sleep(time.Second)
		case TaskTypeExit:
			log.Printf("Received exit signal; worker shutting down\n")
			return
		default:
			log.Printf("Received unknown task type: %v\n", task.Type)
			time.Sleep(time.Second)
		}
	}
}

func GetTask() *Task {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}

	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		log.Printf("Received task: %+v\n", reply.Task)
		return &reply.Task
	} else {
		log.Printf("GetTask RPC call failed\n")
		return nil
	}
}

func ReportTaskCompletion(task *Task) {
	ok := call("Coordinator.ReportTaskCompletion", task, &struct{}{})
	if ok {
		log.Printf("Reported completion of task %d\n", task.TaskNum)
	} else {
		log.Printf("ReportTaskCompletion RPC call failed\n")
	}
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
