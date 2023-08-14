package mr

import (
	"fmt"
	"github.com/google/uuid"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// MapTask number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	workerID := uuid.New()
	log.Printf("Initiating worker with id %v \n", workerID)
	for {
		func() {
			taskToDo := callGetMapTask(workerID)

			if len(taskToDo.Task.Name) == 0 {
				return
			}
			processMapTask(&taskToDo, mapf)
			callMapStatusUpdate(taskToDo.Task, workerID)
		}()

		go func() {
			reduceTask := callGetReduceTask(workerID)
			if len(reduceTask.Task.MappedFiles) == 0 {
				return
			}
			processReduceTask(&reduceTask, reducef)
			callReduceStatusUpdate(reduceTask.Task, workerID)
		}()
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
