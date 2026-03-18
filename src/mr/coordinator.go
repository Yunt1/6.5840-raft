package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	Idle = iota
	InProgress
	Completed
) //任务状态

type Task struct {
	TaskID   int
	FileName string // map任务使用文件名；reduce任务可为空
	Status   int    // Idle, InProgress, Completed
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	phase       string
	mapTasks    []Task
	reduceTasks []Task
	nReduce     int //reduce任务数量

}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase == "map" { //分配给map
		for i := range c.mapTasks {
			if c.mapTasks[i].Status == Idle {
				c.mapTasks[i].Status = InProgress
				c.mapTasks[i].StartTime = time.Now()
				reply.TaskID = c.mapTasks[i].TaskID
				reply.FileName = c.mapTasks[i].FileName
				reply.TaskType = "map"
				reply.NReduce = c.nReduce
				reply.TotalMapTasks = len(c.mapTasks)
				return nil
			}
		}
		for i:=range c.mapTasks{
			if c.mapTasks[i].Status==InProgress&&time.Since(c.mapTasks[i].StartTime) > 10*time.Second{
				c.mapTasks[i].StartTime=time.Now()
				reply.TaskID = c.mapTasks[i].TaskID
				reply.FileName = c.mapTasks[i].FileName
				reply.TaskType = "map"
				reply.NReduce = c.nReduce
				reply.TotalMapTasks = len(c.mapTasks)
				return nil
			}
			
		}
		allDone := true
		for i := range c.mapTasks {
			if c.mapTasks[i].Status != Completed {
				allDone = false
				break
			}
		}
		if allDone {
			c.phase = "reduce"
		} else {
			reply.TaskType = "wait"
			return nil
		}
	}
	// fmt.Printf("reduce开始")
	if c.phase == "reduce" { //分配给reduce
		for i := range c.reduceTasks {
			if c.reduceTasks[i].Status == Idle {
				c.reduceTasks[i].Status = InProgress
				c.reduceTasks[i].StartTime = time.Now()
				reply.TaskID = c.reduceTasks[i].TaskID
				reply.TaskType = "reduce"
				reply.TotalMapTasks = len(c.mapTasks)
				return nil
			}
		}
		for i:=range c.reduceTasks{
			if c.reduceTasks[i].Status==InProgress&&time.Since(c.reduceTasks[i].StartTime) > 10*time.Second{
				c.reduceTasks[i].StartTime=time.Now()
				reply.TaskID = c.reduceTasks[i].TaskID
				reply.TaskType = "reduce"
				reply.TotalMapTasks = len(c.mapTasks)
				return nil
			}
		}
		allDone := true
		for i := range c.reduceTasks {
			if c.reduceTasks[i].Status != Completed {
				allDone = false
				break
			}
		}
		if allDone {
			c.phase = "done"
		} else {
			reply.TaskType = "wait"
			return nil
		}
	}
	return fmt.Errorf("all tasks completed")
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
	rpc.Register(c)//注册rpc服务
	rpc.HandleHTTP()//将rpc服务绑定到http
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()//获取socket文件名
	os.Remove(sockname)//删除旧的socket文件
	l, e := net.Listen("unix", sockname)//监听socket
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)//启动http服务
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == "done"
}

func (c *Coordinator) TaskDone(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase == "map" { //分配给map
		if args.TaskID>=0&&args.TaskID<len(c.mapTasks){
			c.mapTasks[args.TaskID].Status = Completed
		}
	}else{
		if args.TaskID>=0&&args.TaskID<len(c.reduceTasks){
			c.reduceTasks[args.TaskID].Status = Completed
		}
	}
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
		phase:   "map",
	}
	c.mapTasks = make([]Task, len(files))
	for i := range files {
		c.mapTasks[i] = Task{
			TaskID:   i,
			FileName: files[i],
			Status:   Idle,
		}
	}
	c.reduceTasks = make([]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			TaskID: i,
			Status: Idle,
		}
	}

	// Your code here.

	c.server()
	return &c
}
