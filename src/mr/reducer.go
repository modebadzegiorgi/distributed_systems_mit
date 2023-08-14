package mr

import (
	"bufio"
	"fmt"
	"github.com/google/uuid"
	"os"
	"sort"
	"strings"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func callReduceStatusUpdate(task ReduceTask, WorkerID uuid.UUID) {
	args := ReduceJobStatusArgs{Task: task, Type: "reduce", WorkerID: WorkerID}
	reply := ReduceJobStatusReply{}
	ok := call("Coordinator.ReduceJobStatusUpdate", &args, &reply)

	if !ok {
		fmt.Println("Could not connect to coordinator")
	}
}

func callGetReduceTask(WorkerID uuid.UUID) (task ReduceJobRequestReply) {
	args := ReduceJobRequestArgs{WorkerID: WorkerID}
	reply := ReduceJobRequestReply{}

	ok := call("Coordinator.GetReduceTask", &args, &reply)

	if ok {
		if len(reply.Task.MappedFiles) == 0 {
			time.Sleep(5 * time.Second)
		}
		return reply
	} else {
		return ReduceJobRequestReply{}
	}

}

func processReduceTask(task *ReduceJobRequestReply, reducef func(string, []string) string) {
	var kvs []KeyValue
	for _, file := range task.Task.MappedFiles {
		kvsFromFile, err := convertFileToKvs(file)
		if err != nil {
			task.Task.Status = FAILED
			return
		}
		kvs = append(kvs, kvsFromFile...)
	}
	sort.Sort(ByKey(kvs))

	oname := fmt.Sprintf("mr-out-%v", task.Task.Number)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)

		i = j
	}

	ofile.Close()

	task.Task.Status = SUCCESS

	cleanUpIntermediateFiles(task.Task.MappedFiles)
}

func convertFileToKvs(fileName string) ([]KeyValue, error) {
	readFile, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("did not find the file %v", err)
	}
	defer readFile.Close()
	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Split(bufio.ScanLines)

	var kvs []KeyValue
	for fileScanner.Scan() {
		line := fileScanner.Text()
		s := strings.Fields(line)
		kv := KeyValue{Key: s[0], Value: s[1]}
		kvs = append(kvs, kv)
	}

	return kvs, nil
}

func cleanUpIntermediateFiles(fileNames []string) {
	for _, file := range fileNames {
		os.Remove(file)
	}
}
