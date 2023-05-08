package mr

import (
	"sync"
	"time"
)

const (
	TaskRetryTimes       = 3
	TaskTimeout          = 60 * 10 //ygh: 超时时间为10分钟
	MonitorSleepInterval = 5
	MonitorHeartInterval = 30
)

const (
	StrNull = "null"
	IntNull = -1
)

const (
	StrStatusNoready  = "noready"
	StrStatusPending  = "pending"
	StrStatusRunning  = "running"
	StrStatusFinished = "finished"
	StrStatusFailed   = "failed"
)

const (
	StrPhaseMap                 = "Map"
	StrPhaseReduce              = "Reduce"
	StrEventPhaseMapFinished    = "phase_map_finshed"
	StrEventPhaseReduceFinished = "phase_reduce_finished"
	StrEventCoordinatorFinished = "coordinator_finished"
	StrEventCoordinatorStart    = "coordinator_start"
)

var CoorStatusSlice = []string{StrStatusRunning, StrStatusFinished, StrStatusFailed}
var PhaseStatusSlice = []string{StrStatusNoready, StrStatusRunning, StrStatusFinished, StrStatusFailed}
var TaskStatusSlice = []string{StrStatusPending, StrStatusRunning, StrStatusFinished, StrStatusFailed}

type KeyValueAuto struct {
	Key   string
	Value interface{}
}

type Task struct {
	TaskID    string
	PhaseName string

	Status string

	StartTime  int64
	FinishTime int64
	UpdateTime int64

	Input  KeyValueAuto
	Output KeyValueAuto
}

type StatusQueue struct {
	Pending  StringQueue    //ygh: 标准的队列操作
	Running  map[string]int //ygh: 1.添加 2.任意位置元素的删除(finish or timeout)
	Finished StringQueue    //ygh: 添加
	Failed   map[string]int //ygh: 查看元素数量
}

type Phase struct {
	Name    string
	Status  string
	Tasknum int
	Tasks   map[string]*Task
	Queue   StatusQueue
}

type Coordinator struct {
	// Your definitions here.
	// Phases []*Phase
	Phases   map[string]*Phase
	Status   string
	EventMap map[string]EventHandler
}

type Event struct {
	Time time.Time
}

type EventFunc func(*Coordinator, Event) error

type EventHandler struct {
	Name        string
	HandlerFunc EventFunc
	EventChan   chan Event
}

// type T string

type StringQueue struct {
	sync.Mutex
	list []string
}

func (q *StringQueue) Push(v string) {
	q.Lock()
	q.list = append(q.list, v)
	q.Unlock()
}

func (q *StringQueue) Pop() string {
	q.Lock()
	head := (q.list)[0]
	q.list = (q.list)[1:]
	q.Unlock()
	return head
}

func (q *StringQueue) IsImpty() bool {
	return len(q.list) == 0
}

func (q *StringQueue) Len() int {
	return len(q.list)
}
