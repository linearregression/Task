package task

import (
	"encoding/json"
	"errors"
	"github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/sqs"
	"github.com/mitchellh/mapstructure"
	"github.com/wayt/happyngine/env"
	"github.com/wayt/happyngine/log"
	"reflect"
	"runtime"
	"sync"
	"time"
)

// Tasks map
var tasks map[string]*Task = make(map[string]*Task)
var batchSize = 4
var queue *sqs.Queue

func init() {

	var auth = aws.Auth{
		AccessKey: env.Get("AWS_ACCESS_KEY_ID"),
		SecretKey: env.Get("AWS_SECRET_ACCESS_KEY"),
	}

	regionName := env.Get("AWS_DEFAULT_REGION")
	conn := sqs.New(auth, aws.Regions[regionName])

	var err error
	queue, err = conn.GetQueue(env.Get("AWS_SQS_QUEUE"))
	if err != nil {
		panic(err)
	}

	if cnt := env.GetInt("TASK_BATCH_SIZE"); cnt > 0 {
		batchSize = cnt
	}

	go taskRunner()
}

// Task model
type Task struct {
	Name string
	fv   reflect.Value // Kind() == reflect.Func
}

// New task, name should be unique
// i is a function which will be call on event
func New(name string, i interface{}) *Task {

	if _, ok := tasks[name]; ok {
		panic(errors.New("duplicate task name: " + name))
	}

	t := &Task{
		Name: name,
		fv:   reflect.ValueOf(i),
	}

	f := t.fv.Type()
	if f.Kind() != reflect.Func {
		panic(errors.New("not a function"))
	}

	tasks[name] = t

	return t
}

// Task instance
type TaskSchedule struct {
	Name   string        `json:"name"`
	Params []interface{} `json:"params,omitempty"`
}

// Schedule with a delay, /!\ max 900 seconds (15 min)
func (t *Task) ScheduleWithDelay(seconds int64, args ...interface{}) error {

	sc := &TaskSchedule{
		Name:   t.Name,
		Params: args,
	}

	data, err := json.Marshal(sc)
	if err != nil {
		return err
	}

	if seconds > 0 {
		_, err = queue.SendMessageWithDelay(string(data), seconds)
	} else {
		_, err = queue.SendMessage(string(data))
	}
	return err
}

// Schedule a task to be executed as soon as possible
func (t *Task) Schedule(args ...interface{}) error {

	return t.ScheduleWithDelay(0, args...)
}

// Call task with params, /!\ may panic
func (t *Task) call(args ...interface{}) error {

	ft := t.fv.Type()
	in := []reflect.Value{}

	if len(args) != ft.NumIn() {
		panic("Invalid number of argument(s)")
	}

	if ft.NumIn() > 0 {
		for i, arg := range args {
			var v reflect.Value
			if arg != nil {

				paramType := ft.In(i)

				tmp := reflect.New(paramType)
				mapstructure.Decode(arg, tmp.Interface())

				v = tmp.Elem()
			} else {
				// Task was passed a nil argument, so we must construct
				// the zero value for the argument here.
				n := len(in) // we're constructing the nth argument
				var at reflect.Type
				if !ft.IsVariadic() || n < ft.NumIn()-1 {
					at = ft.In(n)
				} else {
					at = ft.In(ft.NumIn() - 1).Elem()
				}
				v = reflect.Zero(at)
			}
			in = append(in, v)
		}
	}

	t.fv.Call(in)

	return nil
}

// Task runner daemon loop
func taskRunner() {
	log.Debugln("worker: Start polling")

	for {
		resp, err := queue.ReceiveMessage(batchSize)
		if err != nil {
			log.Errorln(err)
			continue
		}

		messages := resp.Messages

		if len(messages) > 0 {
			run(messages)
		} else {
			time.Sleep(time.Millisecond * 500)
		}
	}

	log.Debugln("worker: Leaving polling")
}

func run(messages []sqs.Message) {

	numMessages := len(messages)
	log.Debugln("worker: Received", numMessages, " tasks")

	var wg sync.WaitGroup
	wg.Add(numMessages)
	for i := range messages {
		go func(m *sqs.Message) {
			// launch goroutine
			defer func() {
				if err := recover(); err != nil {

					trace := make([]byte, 2048)
					runtime.Stack(trace, true)

					log.Criticalln("worker:", err, string(trace))
				}
				wg.Done()
			}()

			if err := handleMessage(m); err != nil {
				log.Errorln("worker:", err)
			}
		}(&messages[i])
	}

	wg.Wait()
}

func handleMessage(m *sqs.Message) error {

	ts := new(TaskSchedule)
	if err := json.Unmarshal([]byte(m.Body), ts); err != nil {
		return err
	}

	t, ok := tasks[ts.Name]
	if !ok {
		return errors.New("Unknow task: " + ts.Name)
	}

	log.Debugln("worker: call:", ts.Name)
	t.call(ts.Params...)

	_, err := queue.DeleteMessage(m)
	return err
}
