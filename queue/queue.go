package queue

import (
	"context"
	"fmt"

	"github.com/assembly-hub/basics/workpool"
	"github.com/assembly-hub/log"
	"github.com/assembly-hub/log/empty"

	"github.com/assembly-hub/task/taskfunc"
)

// Queue 无序任务队列
type Queue interface {
	// Logger 设置logger
	Logger(logger log.Log)
	// ConsumeFixed 设置消费函数，函数必须为：funcType
	ConsumeFixed(f funcType) Queue
	// ConsumeFlexible 设置消费函数，推荐此方法, 参数可以为空，无返回值
	// 添加任务，函数格式必须为：
	//    taskfunc(param ...interface{})
	//    或自定义参数
	//    taskfunc(i int, s string, arr []int)
	ConsumeFlexible(f any) Queue
	// AddMsg 添加消息, 数据必须与注册的消费者参数一致
	AddMsg(param ...any) Queue
	// IsFinished 判断是否全部消费
	IsFinished() bool
	// Destruction 销毁队列
	Destruction()
	// OpenFinishNotify 开启任务完成通知，开启之后需要 WatchFinishNotify 监听，否则死锁
	OpenFinishNotify()
	// WatchFinishNotify 监听通知，开启之后需要监听，否则死锁
	WatchFinishNotify() <-chan struct{}
	// BlockWaitFinishNotify 监听通知，开启之后需要监听，否则死锁
	BlockWaitFinishNotify()
}

type queue struct {
	ctx          context.Context
	logger       log.Log
	taskFunc     any
	taskFlexible bool
	wp           workpool.WorkPool
}

func (q *queue) BlockWaitFinishNotify() {
	q.wp.BlockWaitFinishNotify()
}

func (q *queue) OpenFinishNotify() {
	q.wp.OpenFinishNotify()
}

func (q *queue) WatchFinishNotify() <-chan struct{} {
	return q.wp.WatchFinishNotify()
}

func (q *queue) Logger(logger log.Log) {
	q.logger = logger
}

func (q *queue) ConsumeFixed(f funcType) Queue {
	if q.taskFunc != nil {
		q.logger.Warning(q.ctx, "consume task func already exist")
	}
	q.taskFunc = f
	q.taskFlexible = false
	return q
}

func (q *queue) ConsumeFlexible(f any) Queue {
	if q.taskFunc != nil {
		q.logger.Warning(context.Background(), "consume task func already exist")
	}
	m := taskfunc.NewAsyncFuncType(q.ctx, f, q.logger)
	q.taskFunc = m
	q.taskFlexible = true
	return q
}

func (q *queue) AddMsg(param ...any) Queue {
	q.wp.SubmitJob(&workpool.JobBag{
		JobFunc: q.taskBag,
		Params:  param,
	})
	return q
}

func (q *queue) IsFinished() bool {
	return q.wp.IsFinished()
}

func (q *queue) Destruction() {
	q.wp.ShutDownPool()
}

func (q *queue) taskBag(p ...any) {
	defer func() {
		if ex := recover(); ex != nil {
			q.logger.Error(q.ctx, fmt.Sprintf("%v", ex))
		}
	}()

	if q.taskFlexible {
		q.taskFunc.(*taskfunc.AsyncFuncType).Call(p...)
	} else {
		q.taskFunc.(funcType)(p...)
	}
}

func NewQueue(ctx context.Context, concurrency, queueLen int) Queue {
	q := new(queue)
	q.ctx = ctx
	q.logger = empty.NoLog
	q.wp = workpool.NewWorkPool(concurrency, "queue", 0, queueLen)
	return q
}
