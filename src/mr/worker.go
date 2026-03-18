package mr



import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
	
)


// ByKey is a type for sorting KeyValue slices by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		args := TaskArgs{}
		reply := TaskArgs{}
		if !call("Coordinator.RequestTask", &args, &reply) {
			fmt.Println("Coordinator 退出，Worker 结束")
			return
		}
		switch reply.TaskType {
		case "map":
			doMapTask(reply.FileName, reply.TaskID, reply.NReduce, mapf)
		case "reduce":
			doReduceTask(reply.TaskID, reply.TotalMapTasks, reducef)
		case "wait": 
			time.Sleep(time.Second)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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



func doMapTask(fileName string, taskID int, nReduce int, mapf func(string, string) []KeyValue) {//当前是第taskID个map任务，共有nReduce个reduce任务
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, _:= ioutil.ReadAll(file)
	file.Close()//读取文件
	kva := mapf(fileName, string(content))//创造哈希表
	partitions := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		partition := ihash(kv.Key) % nReduce
		partitions[partition] = append(partitions[partition], kv)
	}//创造分区
	for i:=0; i<nReduce; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", taskID, i)
		file, _ := os.Create(fileName)
		enc:=json.NewEncoder(file)
		for _, kv := range partitions[i] {
			enc.Encode(&kv)
		}
		file.Close()
	}//写入文件（分区后）
	argsDone := TaskArgs{TaskID: taskID, TaskType: "map"}
	replyDone := TaskArgs{}
	call("Coordinator.TaskDone", &argsDone, &replyDone)
}


func doReduceTask(reduceTaskID int, totalMapTasks int, reducef func(string, []string) string) {//共有totalMapTasks个map任务，当前是第reduceTaskID个reduce任务
	kva:=[]KeyValue{}
	for i:=0; i<totalMapTasks; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", i, reduceTaskID)
		file, err := os.Open(fileName)
		if err != nil {
			continue
		}//找到对应reduce任务的文件
		dec:=json.NewDecoder(file)
		for {
			var kv KeyValue
			if err:=dec.Decode(&kv); err!=nil {
				break
			}
			kva = append(kva, kv)
		}//读取文件
		file.Close()
	}
	sort.Sort(ByKey(kva))//排序
	outFileName:=fmt.Sprintf("mr-out-%d", reduceTaskID)
	outFile, _:=os.Create(outFileName)//写入文件
	// enc:=json.NewEncoder(outFile)
	i:=0
	for i<len(kva) { //合并相同key的value
		j:=i+1
		for j<len(kva) && kva[j].Key==kva[i].Key {
			j++
		}
		values:=[]string{}
		for k:=i; k<j; k++ {
			values = append(values, kva[k].Value)
		}
		output:=reducef(kva[i].Key, values)
		// enc.Encode(KeyValue{kva[i].Key, output})
		fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, output)
		i=j
	}
	outFile.Close()
	argsDone := TaskArgs{TaskID: reduceTaskID, TaskType: "reduce"}
	replyDone := TaskArgs{}
	call("Coordinator.TaskDone", &argsDone, &replyDone)//通知coordinator任务完成
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
