package mr

import (
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"time"
)

type Func = func(interface{}, interface{}) interface{}

var FuncList map[string]Func
var MonitorHeart bool = true

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func CheckHeartbeat() {
	for {
		args, reply := HeartbeatRequest{}, HeartbeatResponse{}
		ok := call2("Coordinator.Heartbeat", &args, &reply)
		if !ok {
			// fmt.Printf("call failed!\n")
			MonitorHeart = false
		}
		time.Sleep(MonitorHeartInterval)
	}
}

// main/mrworker.go calls this function.
// func Worker(mapf func(string, string) []KeyValue,
//
//	reducef func(string, []string) string) {
func Worker(mapf Func, reducef Func) {
	// gob.RegisterName("[]KeyValueAuto", []KeyValueAuto{})
	gob.Register(KeyValueAuto{})
	gob.Register([]KeyValueAuto{})
	FuncList = make(map[string]Func)
	FuncList[StrPhaseMap] = mapf
	FuncList[StrPhaseReduce] = reducef

	//ygh: Heartbeat
	go CheckHeartbeat()

	for MonitorHeart {
		time.Sleep(time.Second * 5)
		args, reply := GetTaskRequest{}, GetTaskResponse{}
		ok := call2("Coordinator.GetTask", &args, &reply)
		if !ok {
			// fmt.Printf("call failed!\n")
			continue
		}

		task := reply.Task
		fmt.Printf("\n start %#v", task.Input.Key)
		out := FuncList[task.PhaseName](task.Input.Key, task.Input.Value)
		out = out
		fmt.Printf("\n start %#v", task.Input.Key)
		// out := FuncList[task.PhaseName].(Func)(task.Input.Key, task.Input.Value)
		task.Output = KeyValueAuto{
			Key:   task.Input.Key,
			Value: out,
		}
		task.Status = StrStatusFinished

		args2, reply2 := BackTaskRequest{
			Task: task,
		}, BackTaskResponse{}
		fmt.Printf("\n start %#v", task.Input.Key)
		ok = call2("Coordinator.BackTask", &args2, &reply2)
		fmt.Printf("\n start %#v", task.Input.Key)
		if !ok {
			// fmt.Printf("call failed!\n")
			continue
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
func call2(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	// c, err := rpc.DialHTTP("unix", sockname)
	c, err := net.Dial("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	// defer c.Close()

	client := rpc.NewClientWithCodec(jsonrpc.NewClientCodec(c))
	err = client.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
