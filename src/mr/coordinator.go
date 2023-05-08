package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
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
		if phase.Status != StrStatusRunning {
			continue
		}
		if phase.Queue.Pending.IsImpty() {
			continue
		}
		taskid := phase.Queue.Pending.Pop()
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

	task.Status = StrStatusRunning
	task.StartTime = time.Now().Unix()
	task.UpdateTime = task.StartTime

	reply.Task = *task
	return nil
}

func (c *Coordinator) BackTask(args *BackTaskRequest, reply *BackTaskResponse) error {
	p := c.Phases[args.Task.PhaseName]
	t := p.Tasks[args.Task.TaskID]

	t.Status = args.Task.Status
	t.Output = args.Task.Output
	t.UpdateTime = time.Now().Unix()
	if t.Status == StrStatusFinished {
		t.FinishTime = t.UpdateTime
	}
	return nil
}

func (c *Coordinator) Heartbeat(args *HeartbeatRequest, reply *HeartbeatResponse) error {
	fmt.Printf("heart beat")
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
			if phase.Queue.Finished.Len() == phase.Tasknum {
				phase.Status = StrStatusFinished
				if phase.Name == StrPhaseMap { //ygh: 触发Event，是否有更好的触发方式
					c.EventMap[StrEventPhaseMapFinished].EventChan <- Event{}
				}
			} else {
				// for _, num := range phase.Queue.Failed {
				// 	if num > TaskRetryTimes {
				// 		phase.Status = StrStatusFailed
				// 		continue
				// 	}
				// }
				if phase.Queue.Pending.Len()+len(phase.Queue.Running) > 0 {
					phase.Status = StrStatusRunning
				} else {
					phase.Status = StrStatusNoready
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
				if task.Status != StrStatusRunning { //ygh: 暂时只对正在运行的超时任务做管理
					continue
				}
				if GetUnixTimeNow()-task.StartTime > TaskTimeout { //ygh: 超时
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
		time.Sleep(MonitorSleepInterval)
	}
}

// ygh: 组织task之间的依赖关系
// 1. 以指针形式将多个phase串接起来，一个phase结束，下一个phase开始
// 2. 定义每个phase内的事件驱动函数，该phase对象的某些事件触发新建task
func (c *Coordinator) ManageEventHandler() {
	for {
		for _, ehandler := range c.EventMap {
			ehandler.HandlerFunc(c, <-ehandler.EventChan)
		}
	}
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
		p.Tasks[t.TaskID] = &t
		p.Queue.Pending.Push(t.TaskID)
	}
	return nil
}

func (c *Coordinator) AddEventHandler(name string, handlerfunc EventFunc) error {
	c.EventMap[name] = EventHandler{
		Name:        name,
		HandlerFunc: handlerfunc,
		EventChan:   make(chan Event),
	}
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	kvlist, err := GetMapKeyValueList(files)
	if err != nil {
		return nil
	}
	c := Coordinator{
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
	go c.ManageCoorStatus()
	go c.ManagePhaseStatus()
	go c.ManageTaskStatus()
	go c.ManageEventHandler()

	//ygh: start event
	c.EventMap[StrEventCoordinatorStart].EventChan <- Event{}

	c.server()
	return &c
}
