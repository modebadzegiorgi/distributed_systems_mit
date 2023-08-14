package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

type MapTask struct {
	Name           string
	Status         JobStatus
	Number         int
	GeneratedFiles []string
}

type ReduceTask struct {
	MappedFiles []string
	Number      int
	Status      JobStatus
}

type Coordinator struct {
	mapTasks     []MapTask
	tasksReduced bool
	reduceTasks  []ReduceTask
	nReduce      int
	mu           sync.Mutex
}

func (c *Coordinator) GetMapTask(args *MapJobRequestArgs, reply *MapJobRequestReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.hasMapTask() {
		for _, taskToDo := range c.mapTasks {
			if taskToDo.Status == TODO {
				reply.Task = taskToDo
				reply.NReduce = c.nReduce
				c.mapTasks[taskToDo.Number].Status = RUNNING
				log.Printf("Sent Map %v task to worker %v", taskToDo.Number, args.WorkerID)
				break
			}
		}
	}
	return nil
}

func (c *Coordinator) GetReduceTask(args *ReduceJobRequestArgs, reply *ReduceJobRequestReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.finishedMapTask() {
		return nil
	}

	if !c.tasksReduced {
		c.generateReduceTasks()
	}

	if c.hasReduceTask() {
		for _, taskToDo := range c.reduceTasks {
			if taskToDo.Status == TODO {
				reply.Task = taskToDo
				c.reduceTasks[taskToDo.Number].Status = RUNNING
				log.Printf("Sent Reduce task %v to worker %v", taskToDo.Number, args.WorkerID)
				break
			}
		}
	}

	return nil
}

func (c *Coordinator) generateReduceTasks() {

	for i := 0; i < c.nReduce; i++ {
		reduceTask := ReduceTask{}
		reduceTask.Number = i
		reduceTask.Status = TODO

		for _, mapTask := range c.mapTasks {
			for _, mappedFile := range mapTask.GeneratedFiles {
				reduceJobNumber, err := strconv.Atoi(mappedFile[len(mappedFile)-1:])
				if err != nil {
					log.Println("Last Digit of file is not int", err)
				}
				if reduceJobNumber == reduceTask.Number {
					reduceTask.MappedFiles = append(reduceTask.MappedFiles, mappedFile)
					break
				}
			}
		}

		c.reduceTasks = append(c.reduceTasks, reduceTask)
	}

	c.tasksReduced = true
}

func (c *Coordinator) mapTaskInReduce(taskNumber int) bool {
	for _, reduceTask := range c.reduceTasks {
		if reduceTask.Number == taskNumber {
			return true
		}
	}
	return false
}

func (c *Coordinator) MapJobStatusUpdate(args *MapJobStatusArgs, reply *MapJobStatusReply) error {
	c.updateMapJobStatus(args.Task)
	log.Printf("Received Map Job %v update from worker %v - Status : %v", args.Task.Number, args.WorkerID, args.Task.Status)
	return nil
}

func (c *Coordinator) ReduceJobStatusUpdate(args *ReduceJobStatusArgs, reply *MapJobStatusReply) error {
	c.updateReduceJobStatus(args.Task)
	log.Printf("Received Reduce Job %v update from worker %v - Status : %v", args.Task.Number, args.WorkerID, args.Task.Status)
	return nil
}

func (c *Coordinator) updateMapJobStatus(task MapTask) {
	c.mapTasks[task.Number] = task
}

func (c *Coordinator) updateReduceJobStatus(task ReduceTask) {
	c.reduceTasks[task.Number] = task
}

// helper funcs
func (c *Coordinator) hasMapTask() bool {
	for _, task := range c.mapTasks {
		if task.Status == TODO {
			return true
		}
	}
	return false
}

func (c *Coordinator) finishedMapTask() bool {
	for _, task := range c.mapTasks {
		if task.Status != SUCCESS {
			return false
		}
	}
	return true
}

func (c *Coordinator) hasReduceTask() bool {
	for _, task := range c.reduceTasks {
		if task.Status == TODO {
			return true
		}
	}
	return false
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
	if !c.tasksReduced {
		return false
	}

	for _, reduceTask := range c.reduceTasks {
		if reduceTask.Status != SUCCESS {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	var tasksToDo []MapTask
	for i, file := range files {
		tasksToDo = append(tasksToDo, MapTask{Number: i, Name: file, Status: TODO})
	}
	c := Coordinator{mapTasks: tasksToDo, nReduce: nReduce}
	c.server()
	return &c
}
