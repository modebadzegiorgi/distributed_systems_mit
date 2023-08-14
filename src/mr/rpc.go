package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	gUUID "github.com/google/uuid"
	"os"
	"strconv"
)

type JobStatus int

const (
	FAILED JobStatus = iota
	RUNNING
	SUCCESS
	TODO
)

// MapTask request
type MapJobRequestArgs struct {
	WorkerID gUUID.UUID
}

type MapJobRequestReply struct {
	Task    MapTask
	NReduce int
}

// type job status
type MapJobStatusArgs struct {
	WorkerID gUUID.UUID
	Task     MapTask
	Type     string
}

type MapJobStatusReply struct {
}

// Reduce request

type ReduceJobRequestArgs struct {
	WorkerID gUUID.UUID
}

type ReduceJobRequestReply struct {
	Task ReduceTask
}

type ReduceJobStatusArgs struct {
	WorkerID gUUID.UUID
	Task     ReduceTask
	Type     string
}

type ReduceJobStatusReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
