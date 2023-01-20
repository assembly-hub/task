// Package execute
package execute

import (
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"sync"

	"github.com/assembly-hub/task/taskfunc"
)

type Executor interface {
	// ClearTask 清除已有任务
	ClearTask() error
	// AddTask 添加需要执行的任务，函数必须为：executorFunc
	AddTask(f executorFunc, param ...interface{}) Executor
	// AddSimpleTask 推荐此方法,
	// 添加任务，函数格式必须为：taskfunc(param ...interface{}) (interface{}, error) or 自定义参数 taskfunc(i int, s string, arr []int) (interface{}, error)
	AddSimpleTask(f interface{}, param ...interface{}) Executor
	// ExecuteTask 执行任务，返回任务结果，不返回任务error以及执行error
	ExecuteTask() ([]interface{}, error)
	// ExecuteTaskWithErr 执行任务，返回任务结果和任务error以及执行error
	ExecuteTaskWithErr() ([]interface{}, []error, error)
}

type exeTask struct {
	taskList []*taskFunc
	taskName string
	wg       sync.WaitGroup
	result   []interface{}
	taskErr  []error
}

func (t *exeTask) taskFuncBag(i int, f *taskFunc) (err error) {
	defer func() {
		if p := recover(); p != nil {
			log.Printf("error: %v\n%s", p, string(debug.Stack()))
			err = fmt.Errorf("%v", p)
		}
	}()
	var r interface{}
	if f.f != nil {
		r, err = f.f(f.p...)
	} else if f.m != nil {
		r, err = f.m.Call(f.p...)
	} else {
		log.Printf("task taskfunc error\n%s", string(debug.Stack()))
	}
	if err != nil {
		log.Printf("task error: %v\n%s", err, string(debug.Stack()))
	}
	t.result[i] = r
	return
}

func NewExecutor(taskName string) Executor {
	task := new(exeTask)
	task.taskName = taskName
	task.wg = sync.WaitGroup{}
	return task
}

func (t *exeTask) ClearTask() error {
	t.taskList = nil
	return nil
}

func (t *exeTask) AddTask(f executorFunc, param ...interface{}) Executor {
	t.taskList = append(t.taskList, &taskFunc{
		f: f,
		p: param,
	})
	return t
}

func (t *exeTask) AddSimpleTask(f interface{}, param ...interface{}) Executor {
	m := taskfunc.NewConcurrencyFuncType(f)
	t.taskList = append(t.taskList, &taskFunc{
		m: m,
		p: param,
	})
	return t
}

func (t *exeTask) ExecuteTask() ([]interface{}, error) {
	defer func() {
		if p := recover(); p != nil {
			log.Printf("task[%s] err[%v]\n%s", t.taskName, p, string(debug.Stack()))
		}
		t.taskList = nil
		t.result = nil
		t.taskErr = nil
	}()

	taskLen := len(t.taskList)
	if taskLen <= 0 {
		return nil, errors.New("暂无可执行任务")
	}

	t.result = make([]interface{}, taskLen)
	t.taskErr = make([]error, taskLen)

	t.wg.Add(len(t.taskList))
	for i, val := range t.taskList {
		go func(index int, fun *taskFunc) {
			defer t.wg.Done()
			err := t.taskFuncBag(index, fun)
			if err != nil {
				t.taskErr[index] = err
				log.Printf("task error: %v\n%s", err, string(debug.Stack()))
			}
		}(i, val)
	}
	t.wg.Wait()
	return t.result, nil
}

func (t *exeTask) ExecuteTaskWithErr() ([]interface{}, []error, error) {
	defer func() {
		if p := recover(); p != nil {
			log.Printf("task[%s] err[%v]\n%s", t.taskName, p, string(debug.Stack()))
		}
		t.taskList = nil
		t.result = nil
		t.taskErr = nil
	}()

	taskLen := len(t.taskList)
	if taskLen <= 0 {
		return nil, nil, errors.New("暂无可执行任务")
	}

	t.result = make([]interface{}, taskLen)
	t.taskErr = make([]error, taskLen)

	t.wg.Add(len(t.taskList))
	for i, val := range t.taskList {
		go func(index int, fun *taskFunc) {
			defer t.wg.Done()
			err := t.taskFuncBag(index, fun)
			if err != nil {
				t.taskErr[index] = err
				log.Printf("task error: %v\n%s", err, string(debug.Stack()))
			}
		}(i, val)
	}
	t.wg.Wait()
	return t.result, t.taskErr, nil
}
