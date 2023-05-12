package mr

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)

type ByKey []KeyValueAuto

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var ErrCannotOpenFile = errors.New("cannot open file")

func GetMapKeyValueList(files []string) ([]KeyValueAuto, error) {
	var kvlist []KeyValueAuto
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			// log.Fatalf("cannot open %v", filename)
			return nil, ErrCannotOpenFile
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			// log.Fatalf("cannot read %v", filename)
			return nil, ErrCannotOpenFile
		}
		file.Close()
		kvlist = append(kvlist, KeyValueAuto{
			Key:   filename,
			Value: string(content),
		})
	}
	return kvlist, nil
}

func HandlerMapPhaseFinish(c *Coordinator, e Event) error {
	fmt.Printf("\n\n map phase finish! \n\n")
	intermediate := []KeyValueAuto{}
	for _, task := range c.Phases[StrPhaseMap].Tasks {
		for _, item := range task.Output.Value.([]interface{}) {
			fmt.Printf("%#v", item.(map[string]interface{}))
			newitem := item.(map[string]interface{})
			intermediate = append(intermediate, KeyValueAuto{
				Key:   newitem["Key"].(string),
				Value: newitem["Value"],
			})
		}
	}
	sort.Sort(ByKey(intermediate))
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value.(string))
		}
		kv := KeyValueAuto{
			Key:   intermediate[i].Key,
			Value: values,
		}
		task := Task{
			TaskID:     Getuid(),
			PhaseName:  StrPhaseReduce,
			Status:     StrStatusPending,
			Input:      kv,
			Output:     KeyValueAuto{},
			UpdateTime: GetUnixTimeNow(),
		}
		c.Phases[StrPhaseReduce].Tasknum += 1
		c.Phases[StrPhaseReduce].Tasks[task.TaskID] = &task
		c.Phases[StrPhaseReduce].Queue.Pending.Push(task.TaskID)

		i = j
	}
	c.Phases[StrPhaseReduce].Status = StrStatusRunning

	return nil
}

func HandlerReducePhaseFinish(c *Coordinator, e Event) error {
	oname := "mr-out-0"
	ofile, _ := os.Create(oname)
	for _, task := range c.Phases[StrPhaseReduce].Tasks {
		kv := task.Output
		fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
	}
	c.EventMap[StrEventCoordinatorFinished].EventChan <- Event{}
	return nil
}

func HandlerCoordinatorFinish(c *Coordinator, e Event) error {
	c.Status = StrStatusFinished
	return nil
}

func HandlerCoordinatorStart(c *Coordinator, e Event) error {
	c.Status = StrStatusRunning
	c.Phases[StrPhaseMap].Status = StrStatusRunning
	return nil
}
