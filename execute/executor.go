// Package execute
package execute

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"

	"github.com/assembly-hub/log"
	"github.com/assembly-hub/log/empty"

	"github.com/assembly-hub/task/taskfunc"
)

type Executor interface {
	// Logger 设置logger
	Logger(logger log.Log)
	// Clear 清除已有任务
	Clear()
	// Empty 判断是否有任务
	Empty() bool
	// Count 获取任务数量
	Count() int
	// AddFixed 添加需要执行的任务，函数必须为：executorFunc
	AddFixed(f executorFunc, param ...interface{}) Executor
	// AddFlexible 推荐此方法, 参数可以为空，返回值必须是：any, error，其中 any 可以是 interface 或者其他自定义类型
	// 添加任务，函数格式必须为：
	//    taskfunc(param ...interface{}) (interface{}, error)
	//    或自定义参数
	//    taskfunc(i int, s string, arr []int) (interface{}, error)
	AddFlexible(f interface{}, param ...interface{}) Executor
	// Execute 执行任务，返回任务结果，不返回任务error以及执行error
	Execute(ctx context.Context) ([]interface{}, error)
	// ExecuteWithErr 执行任务，返回任务结果和任务error以及执行error
	ExecuteWithErr(ctx context.Context) ([]interface{}, []error, error)
}

type exeTask struct {
	ctx      context.Context
	taskList []*taskFunc
	taskName string
	wg       sync.WaitGroup
	result   []interface{}
	taskErr  []error
	logger   log.Log
}

func (t *exeTask) Logger(logger log.Log) {
	if logger != nil {
		t.logger = logger
	}
}

func (t *exeTask) taskFuncBag(i int, f *taskFunc) (err error) {
	defer func() {
		if p := recover(); p != nil {
			t.logger.Error(t.ctx, fmt.Sprintf("error: %v\n%s", p, string(debug.Stack())))
			err = fmt.Errorf("%v", p)
		}
	}()
	var r interface{}
	if f.f != nil {
		r, err = f.f(f.p...)
	} else if f.m != nil {
		r, err = f.m.Call(f.p...)
	} else {
		t.logger.Error(t.ctx, fmt.Sprintf("task taskfunc error\n%s", string(debug.Stack())))
	}
	if err != nil {
		t.logger.Error(t.ctx, fmt.Sprintf("task error: %v\n%s", err, string(debug.Stack())))
	}
	t.result[i] = r
	return
}

func NewExecutor(taskName string) Executor {
	task := new(exeTask)
	task.ctx = context.Background()
	task.taskName = taskName
	task.wg = sync.WaitGroup{}
	task.logger = empty.NoLog
	return task
}

func (t *exeTask) Clear() {
	t.taskList = nil
}

func (t *exeTask) Empty() bool {
	return len(t.taskList) == 0
}

func (t *exeTask) Count() int {
	return len(t.taskList)
}

func (t *exeTask) AddFixed(f executorFunc, param ...interface{}) Executor {
	t.taskList = append(t.taskList, &taskFunc{
		f: f,
		p: param,
	})
	return t
}

func (t *exeTask) AddFlexible(f interface{}, param ...interface{}) Executor {
	m := taskfunc.NewConcurrencyFuncType(t.ctx, f, t.logger)
	t.taskList = append(t.taskList, &taskFunc{
		m: m,
		p: param,
	})
	return t
}

func (t *exeTask) Execute(ctx context.Context) ([]interface{}, error) {
	if ctx != nil {
		t.ctx = ctx
	}
	defer func() {
		if p := recover(); p != nil {
			t.logger.Error(t.ctx, fmt.Sprintf("task[%s] err[%v]\n%s", t.taskName, p, string(debug.Stack())))
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
				t.logger.Error(t.ctx, fmt.Sprintf("task error: %v\n%s", err, string(debug.Stack())))
			}
		}(i, val)
	}
	t.wg.Wait()
	return t.result, nil
}

func (t *exeTask) ExecuteWithErr(ctx context.Context) ([]interface{}, []error, error) {
	if ctx != nil {
		t.ctx = ctx
	}
	defer func() {
		if p := recover(); p != nil {
			t.logger.Error(t.ctx, fmt.Sprintf("task[%s] err[%v]\n%s", t.taskName, p, string(debug.Stack())))
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
				t.logger.Error(t.ctx, fmt.Sprintf("task error: %v\n%s", err, string(debug.Stack())))
			}
		}(i, val)
	}
	t.wg.Wait()
	return t.result, t.taskErr, nil
}
