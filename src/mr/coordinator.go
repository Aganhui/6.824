package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"time"
)

var ErrNoAvailableTask = errors.New("no available pending task")

// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) selectTask() (task *Task, err error) {
	for _, phase := range c.Phases {
		// fmt.Printf("phase %#v", phase)
		if phase.Status != StrStatusRunning {
			continue
		}
		if phase.Queue.Pending.IsImpty() {
			continue
		}
		taskid := phase.Queue.Pending.Pop()
		phase.Queue.Running[taskid] = 1
		task = phase.Tasks[taskid]
		return task, nil
	}
	return nil, ErrNoAvailableTask
}

func (c *Coordinator) GetTask(args *GetTaskRequest, reply *GetTaskResponse) error {
	task, err := c.selectTask()
	if err != nil {
		return err
	}
	// fmt.Printf("\n%#v", task.Input.Value)
	fmt.Printf("\n gettask: %s", task.TaskID)

	task.Status = StrStatusRunning
	task.StartTime = time.Now().Unix()
	task.UpdateTime = task.StartTime

	reply.Task = *task
	return nil
}

func (c *Coordinator) BackTask(args *BackTaskRequest, reply *BackTaskResponse) error {
	p := c.Phases[args.Task.PhaseName]
	t := p.Tasks[args.Task.TaskID]
	fmt.Printf("\n backtask: %s", t.TaskID)

	t.Status = args.Task.Status
	t.Output = args.Task.Output
	t.UpdateTime = time.Now().Unix()
	if t.Status == StrStatusFinished {
		t.FinishTime = t.UpdateTime
		p.Queue.Finished.Push(t.TaskID)
	} else if t.Status == StrStatusFailed { //ygh: 如何更好地管理事件？
		// p.Queue.Failed
	}
	return nil
}

func (c *Coordinator) Heartbeat(args *HeartbeatRequest, reply *HeartbeatResponse) error {
	// fmt.Printf("heart beat")
	return nil
}

func (c *Coordinator) ManageCoorStatus() {
	var tmpState string
	for {
		tmpState = StrStatusRunning
		for _, phase := range c.Phases {
			if phase.Status == StrStatusFailed {
				tmpState = StrStatusFailed
				break
			}
			if phase.Status == StrStatusRunning {
				tmpState = StrStatusRunning
				break
			}
		}
		c.Status = tmpState
		if tmpState != StrStatusRunning {
			break
		}
		time.Sleep(MonitorSleepInterval)
	}
}

func (c *Coordinator) ManagePhaseStatus() {
	for {
		for _, phase := range c.Phases {
			fmt.Printf("\n%s: %s(%d/%d)\n", phase.Name, phase.Status, phase.Queue.Finished.Len(), phase.Tasknum)
			switch phase.Status {
			case StrStatusRunning:
				if phase.Queue.Finished.Len() == phase.Tasknum {
					phase.Status = StrStatusFinished
					if phase.Name == StrPhaseMap { //ygh: 触发Event，是否有更好的触发方式
						fmt.Printf("map finish to feed event\n")
						c.EventMap[StrEventPhaseMapFinished].EventChan <- Event{}
					}
					if phase.Name == StrPhaseReduce { //ygh: 触发Event，是否有更好的触发方式
						fmt.Printf("reduce finish to feed event\n")
						c.EventMap[StrEventPhaseReduceFinished].EventChan <- Event{}
					}
				}
			case StrStatusNoready:
				if phase.Queue.Pending.Len()+len(phase.Queue.Running) > 0 {
					phase.Status = StrStatusRunning
				}
			}
		}
		time.Sleep(MonitorSleepInterval)
	}
}

func (c *Coordinator) ManageTaskStatus() {
	for {
		for _, phase := range c.Phases {
			for _, task := range phase.Tasks {
				switch task.Status {
				case StrStatusRunning: //ygh: 暂时只对正在运行的超时任务做管理
					if GetUnixTimeNow()-task.StartTime > int64(TaskTimeout.Seconds()) { //ygh: 超时
						delete(phase.Queue.Running, task.TaskID)
						if num, ok := phase.Queue.Failed[task.TaskID]; ok { //ygh: 多次失败
							phase.Queue.Failed[task.TaskID] = num + 1
							if num+1 < TaskRetryTimes { //ygh: 失败次数未超过limit
								task.Status = StrStatusPending
								phase.Queue.Pending.Push(task.TaskID)
							} else { //ygh: 失败次数超过limit
								task.Status = StrStatusFailed
								phase.Status = StrStatusFailed //ygh: 此时认为phase和整个任务都失败
								c.Status = StrStatusFailed
							}
						} else { //ygh: 第一次失败
							task.Status = StrStatusPending
							phase.Queue.Pending.Push(task.TaskID)
						}
					}
				}
			}
		}
		time.Sleep(MonitorSleepInterval)
	}
}

func (c *Coordinator) StartEventHandler(eh *EventHandler) {
	for {
		fmt.Printf("\nhandler: %s\n", eh.Name)
		eh.HandlerFunc(c, <-eh.EventChan)
	}
}

// ygh: 组织task之间的依赖关系
// 1. 以指针形式将多个phase串接起来，一个phase结束，下一个phase开始
// 2. 定义每个phase内的事件驱动函数，该phase对象的某些事件触发新建task
func (c *Coordinator) ManageEventHandler() {
	fmt.Printf("\nhandler map: %#v\n", c.EventMap)
	for _, ehandler := range c.EventMap {
		go c.StartEventHandler(&ehandler) //ygh: 这样有问题，ehandler的地址是确定的
	}
}

// func (c *Coordinator) ManageEventHandler() {
// 	for {
// 		for _, ehandler := range c.EventMap {
// 			ehandler.HandlerFunc(c, <-ehandler.EventChan)
// 		}
// 	}
// }

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	fmt.Printf("try to listen unix")
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) server2() {
	// 初始化 RPC 服务端
	server := rpc.NewServer()

	sockname := coordinatorSock()
	os.Remove(sockname)
	fmt.Printf("try to listen unix")
	listener, err := net.Listen("unix", sockname)
	// listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("监听端口失败：%v", err)
	}
	defer listener.Close()

	// 注册处理器
	err = server.Register(c)
	if err != nil {
		log.Fatalf("注册服务处理器失败：%v", err)
	}

	for {
		// 等待并处理客户端连接
		conn, err := listener.Accept()
		if err != nil {
			log.Fatalf("接收客户端连接请求失败: %v", err)
		}

		// 自定义 RPC 编码器：新建一个 jsonrpc 编码器，并将该编码器绑定给 RPC 服务端处理器
		go server.ServeCodec(jsonrpc.NewServerCodec(conn))
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := true
	// for _, phase := range c.Phases {
	// 	if phase.Status != StrStatusFinished {
	// 		ret = false
	// 	}
	// }
	if c.Status == StrStatusRunning {
		ret = false
	}
	return ret
}

func (c *Coordinator) AddPhase(name string, input []KeyValueAuto) error {
	// func (c *Coordinator) AddPhase(name string, input interface{}) error {
	p := Phase{
		Name:   name,
		Status: StrStatusPending,
		Tasks:  make(map[string]*Task),
		Queue: StatusQueue{
			Running: make(map[string]int),
			Failed:  make(map[string]int),
		},
	}
	p.Tasknum = len(input)
	for _, item := range input {
		t := Task{
			TaskID:     Getuid(),
			PhaseName:  name,
			Status:     StrStatusPending,
			Input:      item,
			Output:     KeyValueAuto{},
			StartTime:  IntNull,
			FinishTime: IntNull,
			UpdateTime: IntNull,
		}
		// fmt.Printf("\n%#v\n", t.Input.Value)
		p.Tasks[t.TaskID] = &t
		p.Queue.Pending.Push(t.TaskID)
	}
	c.Phases[name] = &p
	return nil
}

func (c *Coordinator) AddEventHandler(name string, handlerfunc EventFunc) error {
	eh := EventHandler{
		Name:        name,
		HandlerFunc: handlerfunc,
		EventChan:   make(chan Event, 1),
	}
	c.EventMap[name] = eh
	go c.StartEventHandler(&eh)
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// fmt.Printf("get file list")
	kvlist, err := GetMapKeyValueList(files)
	if err != nil {
		return nil
	}
	// fmt.Printf("create coordinator")
	c := Coordinator{
		Status:   StrStatusRunning,
		Phases:   make(map[string]*Phase),
		EventMap: make(map[string]EventHandler),
	}
	if err := c.AddPhase(StrPhaseMap, kvlist); err != nil {
		fmt.Printf("add phase fail: %#v\n", err)
		return nil
	}
	if err := c.AddPhase(StrPhaseReduce, make([]KeyValueAuto, 0)); err != nil {
		fmt.Printf("add phase fail: %#v\n", err)
		return nil
	}
	if err := c.AddEventHandler(StrEventPhaseMapFinished, HandlerMapPhaseFinish); err != nil {
		fmt.Printf("add func handler fail: %#v\n", err)
		return nil
	}
	if err := c.AddEventHandler(StrEventPhaseReduceFinished, HandlerReducePhaseFinish); err != nil {
		fmt.Printf("add func handler fail: %#v\n", err)
		return nil
	}
	if err := c.AddEventHandler(StrEventCoordinatorFinished, HandlerCoordinatorFinish); err != nil {
		fmt.Printf("add func handler fail: %#v\n", err)
		return nil
	}
	if err := c.AddEventHandler(StrEventCoordinatorStart, HandlerCoordinatorStart); err != nil {
		fmt.Printf("add func handler fail: %#v\n", err)
		return nil
	}
	// fmt.Printf("finish create coordinator")
	go c.ManageCoorStatus()
	go c.ManagePhaseStatus()
	go c.ManageTaskStatus()
	// c.ManageEventHandler()

	//ygh: start event
	fmt.Printf("start chan")
	c.EventMap[StrEventCoordinatorStart].EventChan <- Event{}
	// fmt.Printf("start coordinator")

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	// c.server()
	go c.server2()
	return &c
}
