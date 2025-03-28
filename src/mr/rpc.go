package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

type TaskArgs struct {
	FileName      string
	TaskID        int
	TaskType      string // "map", "reduce", 或 "wait"
	NReduce       int    // 分区数，仅 map 任务需要
	TotalMapTasks int    // map 任务总数，仅 reduce 任务需要
}

type TaskReply struct {
	TaskID        int
	FileName      string
	TaskType      string
	NReduce       int
	TotalMapTasks int
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
