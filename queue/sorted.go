package queue

import (
	"context"
	"fmt"

	"github.com/assembly-hub/log"
	"github.com/assembly-hub/log/empty"

	"github.com/assembly-hub/task/taskfunc"
)

// SortedQueue 顺序任务队列，执行顺序与添加顺序一致
type SortedQueue interface {
	// Logger 设置logger
	Logger(logger log.Log)
	// ConsumeFixed 设置消费函数，函数必须为：funcType
	ConsumeFixed(f funcType) SortedQueue
	// ConsumeFlexible 设置消费函数，推荐此方法, 参数可以为空，无返回值
	// 添加任务，函数格式必须为：
	//    taskfunc(param ...interface{})
	//    或自定义参数
	//    taskfunc(i int, s string, arr []int)
	ConsumeFlexible(f any) SortedQueue
	// AddMsg 添加消息, 数据必须与注册的消费者参数一致
	AddMsg(param ...any) SortedQueue
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

type sortedQueue struct {
	ctx          context.Context
	logger       log.Log
	queueLen     int
	taskChan     chan []any
	taskFunc     any
	taskFlexible bool
	taskIdle     bool
	notifyChan   chan struct{}
}

func (s *sortedQueue) BlockWaitFinishNotify() {
	select {
	case <-s.notifyChan:
		return
	}
}

func (s *sortedQueue) OpenFinishNotify() {
	if s.notifyChan == nil {
		s.notifyChan = make(chan struct{})
	}
}

func (s *sortedQueue) WatchFinishNotify() <-chan struct{} {
	if s.notifyChan == nil {
		panic("please first call OpenFinishNotify, then call WatchFinishNotify")
	}
	return s.notifyChan
}

func (s *sortedQueue) IsFinished() bool {
	return s.taskIdle && len(s.taskChan) == 0
}

func (s *sortedQueue) Logger(logger log.Log) {
	s.logger = logger
}

func (s *sortedQueue) ConsumeFixed(f funcType) SortedQueue {
	if s.taskFunc != nil {
		s.logger.Warning(s.ctx, "consume task func already exist")
	}
	s.taskFunc = f
	s.taskFlexible = false
	return s
}

func (s *sortedQueue) ConsumeFlexible(f interface{}) SortedQueue {
	if s.taskFunc != nil {
		s.logger.Warning(context.Background(), "consume task func already exist")
	}
	m := taskfunc.NewAsyncFuncType(s.ctx, f, s.logger)
	s.taskFunc = m
	s.taskFlexible = true
	return s
}

func (s *sortedQueue) AddMsg(param ...any) SortedQueue {
	if s.taskFunc == nil {
		s.logger.Fatal(s.ctx, "before adding a message, a consumption function needs to be bound")
		return nil
	}
	s.taskChan <- param
	return s
}

func (s *sortedQueue) Destruction() {
	close(s.taskChan)
	if s.notifyChan != nil {
		close(s.notifyChan)
	}
}

func (s *sortedQueue) sendFinishNotify() {
	if s.notifyChan != nil {
		if s.taskIdle && len(s.taskChan) == 0 {
			s.notifyChan <- struct{}{}
		}
	}
}

func (s *sortedQueue) run() {
	for {
		s.sendFinishNotify()
		select {
		case p := <-s.taskChan:
			s.taskIdle = false
			s.taskBag(p)
			s.taskIdle = true
		}
	}
}

func (s *sortedQueue) taskBag(p []any) {
	defer func() {
		if ex := recover(); ex != nil {
			s.logger.Error(s.ctx, fmt.Sprintf("%v", ex))
		}
	}()

	if s.taskFlexible {
		s.taskFunc.(*taskfunc.AsyncFuncType).Call(p...)
	} else {
		s.taskFunc.(funcType)(p...)
	}
}

func NewSortedQueue(ctx context.Context, queueLen int) SortedQueue {
	q := new(sortedQueue)
	q.taskIdle = true
	q.ctx = ctx
	q.logger = empty.NoLog
	q.queueLen = queueLen
	if q.queueLen <= 0 {
		q.queueLen = 0
	}
	q.taskChan = make(chan []any, q.queueLen)
	go q.run()
	return q
}
