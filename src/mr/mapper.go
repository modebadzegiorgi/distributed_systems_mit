package mr

import (
	"fmt"
	"github.com/google/uuid"
	"log"
	"os"
	"sync"
	"time"
)

func callGetMapTask(WorkerID uuid.UUID) (task MapJobRequestReply) {
	args := MapJobRequestArgs{WorkerID: WorkerID}
	reply := MapJobRequestReply{}
	ok := call("Coordinator.GetMapTask", &args, &reply)
	if ok {
		if len(reply.Task.Name) == 0 {
			time.Sleep(5 * time.Second)
		}
		return reply
	} else {
		log.Fatalf("call failed! %v \n", reply)
		return MapJobRequestReply{}
	}

}

func callMapStatusUpdate(task MapTask, WorkerID uuid.UUID) {
	args := MapJobStatusArgs{Task: task, Type: "map", WorkerID: WorkerID}
	reply := MapJobStatusReply{}
	ok := call("Coordinator.MapJobStatusUpdate", &args, &reply)

	if !ok {
		fmt.Println("Could not connect to coordinator")
	}
}

func processMapTask(task *MapJobRequestReply, mapf func(string, string) []KeyValue) {
	content, err := os.ReadFile(task.Task.Name)
	if err != nil {
		log.Println("Did not find the file")
		task.Task.Status = FAILED
	}
	kv := mapf(task.Task.Name, string(content))

	var mu sync.Mutex
	if err := writeToFile(kv, task, &mu); err != nil {
		task.Task.Status = FAILED
	}
	task.Task.Status = SUCCESS
}

func writeToFile(kv []KeyValue, task *MapJobRequestReply, mu *sync.Mutex) error {
	filesToWrite := map[int][]KeyValue{}
	for _, val := range kv {
		writeKey := ihash(val.Key)
		filesToWrite[writeKey%task.NReduce] = append(filesToWrite[writeKey%task.NReduce], val)
	}
	mu.Lock()
	defer mu.Unlock()
	for key, val := range filesToWrite {
		oname := fmt.Sprintf("temp-mr-out-%v-%v", task.Task.Number, key)
		task.Task.GeneratedFiles = append(task.Task.GeneratedFiles, oname)
		ofile, err := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Println("Could not Create or open file", err)
		}
		defer ofile.Close()

		for _, kv := range val {
			fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
		}
	}
	return nil
}
