// Package manage
package manage

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/assembly-hub/basics/redis"
	"github.com/assembly-hub/basics/util"
	"github.com/assembly-hub/basics/uuid"
	"github.com/assembly-hub/basics/workpool"
	redis8 "github.com/go-redis/redis/v8"

	"github.com/assembly-hub/task/taskfunc"
)

// 异步任务管理器，要点：
// 1、所有任务必须注册才可调用
// 2、任务参数必须可序列化、反序列化
// 3、任务管理器只保证任务执行，业务逻辑报错需要业务处理

const msgMaxIdleTime = time.Hour * 10

var (
	timeConf = [][]interface{}{
		{"second", 0, 59},
		{"minute", 0, 59},
		{"hour", 0, 23},
		{"day", 1, 31},
		{"month", 1, 12},
	}
	monthDays     = []int{31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	globalManager Manager
	taskLock      sync.Mutex
)

// Manager 所有task管理器方法
type Manager interface {
	// AddTimerTask 添加定时任务
	AddTimerTask(taskName string, args []interface{}, timer Timer)
	// AddIntervalTask 添加间隔任务
	AddIntervalTask(taskName string, args []interface{}, interval int)
	// AddDelayTask 添加延迟任务
	AddDelayTask(taskName string, args []interface{}, delayTime uint)
	// AddSimpleTask 添加即时任务
	AddSimpleTask(taskName string, args []interface{})
	// RegisterTask 注册任务，包括：任务名称以及执行函数，函数类型必须是 funcType
	RegisterTask(taskName string, fun funcType)
	// RegisterHighLevelTask 推荐此方式
	// 注册任务，包括：任务名称以及执行函数，函数类型必须是 taskfunc(param ...interface{}) 或 自定义参数 taskfunc(i int, s string, arr []int)
	RegisterHighLevelTask(taskName string, fun interface{})
	// UnRegisterTask 反注册，删除执行器
	UnRegisterTask(taskName string)
	// RunTaskManager 启动任务管理器
	RunTaskManager()
	// SetNoTaskEffectiveTime 设置任务的有效期（当任务节点任务出现偏差时，检查失败的任务会再次放入执行队列，直到任务时间超过此参数），单位：秒 默认：60
	SetNoTaskEffectiveTime(second int64)
}

type taskMQ struct {
	TaskName   string        `json:"task_name"`
	SubmitTime string        `json:"submit_time"`
	RunTime    string        `json:"run_time"`
	Params     []interface{} `json:"params"`
}

type manager struct {
	// 主服务key
	taskMainKey string
	// 主服务标识
	isTimerServer bool
	// 服务UUID
	serverUUID string
	// 服务注册间隔
	calculateIntervalMS int64

	// 任务标识
	taskLabel string

	// 任务管理器启动有先后，在服务变更时，某个task可能在旧的的node不存在，因此执行不了，
	// 所有需要将任务重新添加到队列，但是需要兼容这个任务可能就是异常的或者是旧的（已下线的任务），永远不能可能执行的情况
	// 因此需要指定此类任务有效时间，单位：秒 默认：60
	taskEffectiveTime int64

	// redis链接
	redis *redis.Redis

	// 定时任务队列
	taskQueue []*funcData
	taskSet   map[string]struct{}

	// 处理资源配置
	threadSize  int
	threadPool  workpool.WorkPool
	taskFuncMap map[string]interface{}

	// redis MQ
	streamKey    string
	groupName    string
	consumerName string

	// redis ZSet
	delayZSetKey string
}

func (t *manager) UnRegisterTask(taskName string) {
	delete(t.taskFuncMap, taskName)
}

func (t *manager) SetNoTaskEffectiveTime(second int64) {
	t.taskEffectiveTime = second
}

func (t *manager) computeDelayTask() {
	go func(t *manager) {
		innerFun := func() {
			defer func() {
				if p := recover(); p != nil {
					log.Println(p, "\n", string(debug.Stack()))
				}
			}()

			tm := util.IntToStr(time.Now().Unix())
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(3)*time.Second)
			memList, err := t.redis.ZRangeByScore(ctx, t.delayZSetKey, "0", tm, 0, 10)
			cancel()

			if err != nil {
				log.Println(err)
				return
			}

			var mem []interface{}
			for _, val := range memList {
				var mp map[string]interface{}
				err = json.Unmarshal([]byte(val), &mp)
				if err != nil {
					log.Println(err)
					continue
				}

				t.addTaskStruct(&taskMQ{
					TaskName:   mp["task_name"].(string),
					SubmitTime: mp["submit_time"].(string),
					RunTime:    mp["run_time"].(string),
					Params:     mp["params"].([]interface{}),
				})
				mem = append(mem, val)
			}

			if len(mem) > 0 {
				ctx, cancel = context.WithTimeout(context.Background(), time.Duration(3)*time.Second)
				_, err = t.redis.ZRem(ctx, t.delayZSetKey, mem...)
				cancel()
				if err != nil {
					log.Println(err)
				}
			}
		}

		for {
			if t.isTimerServer {
				innerFun()
			}
			time.Sleep(time.Millisecond * 200)
		}
	}(t)
}

func (t *manager) addTaskMap(member map[string]interface{}, score float64) {
	val, err := util.Map2JSON(member)
	if err != nil {
		log.Println("addTaskMap, err: ", err)
		panic(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(3)*time.Second)
	_, err = t.redis.ZAdd(ctx, t.delayZSetKey, val, score)
	cancel()
	if err != nil {
		log.Println("addTaskMap, err: ", err)
		panic(err)
	}
}

func (t *manager) computeTask() {
	inner := func(t *manager) {
		innerTask := func(task *manager) {
			defer func() {
				if p := recover(); p != nil {
					log.Println(p, "\n", string(debug.Stack()))
				}
			}()

			sort.Slice(task.taskQueue, func(i, j int) bool {
				return task.taskQueue[i].runTime < task.taskQueue[j].runTime
			})

			needDelCount := 0
			for i := len(task.taskQueue) - 1; i >= 0; i-- {
				if task.taskQueue[i].runTime == math.MaxInt64 {
					needDelCount++
				}
			}

			if needDelCount > 0 {
				task.taskQueue = task.taskQueue[0 : len(task.taskQueue)-needDelCount]
			}

			now := time.Now()

			for i, tk := range task.taskQueue {
				if tk.runTime <= now.Unix() {
					if t.isTimerServer {
						t.addTaskStruct(&taskMQ{
							TaskName:   tk.taskName,
							SubmitTime: util.IntToStr(now.Unix()),
							RunTime:    util.IntToStr(tk.runTime),
							Params:     tk.args,
						})
					}

					if tk.taskType == "interval" {
						task.taskQueue[i].runTime = now.Unix() + int64(task.taskQueue[i].intervalSecond)
					} else if tk.taskType == "timer" {
						nextTime := task.calculateNextRunTime(tk.timer, &now, false)
						if nextTime > 0 {
							task.taskQueue[i].runTime = nextTime
						} else {
							task.taskQueue[i].runTime = math.MaxInt64
						}
					}
				} else {
					break
				}
			}
		}
		for {
			innerTask(t)
			time.Sleep(time.Millisecond * 100)
		}
	}
	go inner(t)
}

func (t *manager) innerTaskFuncBag(params ...interface{}) {
	msgID := params[0].(string)
	streamKey := params[1].(string)
	p := params[3].([]interface{})
	taskName := params[4].(string)

	defer func() {
		cli, ctx := t.redis.Raw()
		_, err := cli.XAck(ctx, streamKey, t.groupName, msgID).Result()
		if err != nil {
			log.Printf("task name: %s, msg id is %s, ack err: %s", taskName, msgID, err.Error())
		}
		_, err = cli.XDel(ctx, streamKey, msgID).Result()
		if err != nil {
			log.Printf("task name: %s, msg id is %s, del err: %s", taskName, msgID, err.Error())
		} else {
			log.Printf("task name: %s, delete msg id is %s", taskName, msgID)
		}
	}()

	log.Printf("task name: %s, msg id is %s", taskName, msgID)
	if fun, ok := params[2].(funcType); ok {
		fun(p...)
	} else if m, ok := params[2].(*taskfunc.AsyncFuncType); ok {
		m.FormatParamCall(p...)
	}
}

func (t *manager) addTaskStruct(taskMq *taskMQ) {
	cli, ctx := t.redis.Raw()
	params, err := util.Interface2JSON(taskMq.Params)
	if err != nil {
		panic("params not to json")
	}

	arg := redis8.XAddArgs{
		Stream: t.streamKey,
		ID:     "*",
		Values: map[string]interface{}{
			"task_name":   taskMq.TaskName,
			"submit_time": taskMq.SubmitTime,
			"run_time":    taskMq.RunTime,
			"params":      params,
		},
	}
	ret := cli.XAdd(ctx, &arg)
	msgID, err := ret.Result()
	if err != nil {
		panic(err)
	}
	log.Println("add msg id: ", msgID)
}

func (t *manager) initDemo() {
	t.taskFuncMap["demo_func"] = func(param ...interface{}) {
		log.Println(param)
	}
}

func (t *manager) initRedisData() {
	t.addTaskStruct(&taskMQ{
		TaskName:   "demo_func",
		SubmitTime: util.IntToStr(time.Now().Unix()),
		Params: []interface{}{
			"hello task manager",
		},
	})

	cli, ctx := t.redis.Raw()
	_, err := cli.XGroupCreate(ctx, t.streamKey, t.groupName, "0").Result()
	if err != nil {
		if !util.EndWith(err.Error(), "Group name already exists", true) {
			log.Println(err)
			panic(err)
		}
	}

	// 设置消费组的起始id
	_, err = cli.XGroupSetID(ctx, t.streamKey, t.groupName, "0").Result()
	if err != nil {
		log.Println(err)
		panic(err)
	}
}

func (t *manager) taskWatch() {
	innerFunc := func(task *manager, args *redis8.XReadGroupArgs) {
		innerTask := func() {
			defer func() {
				if p := recover(); p != nil {
					log.Println(p, "\n", string(debug.Stack()))
				}
			}()

			cli, ctx := task.redis.Raw()
			ret := cli.XReadGroup(ctx, args)
			result, err := ret.Result()
			if err != nil {
				task.initRedisData()
				panic(err)
			}

			for _, val := range result {
				msgList := val.Messages
				for _, msg := range msgList {
					log.Println("msg: ", msg)

					var arr []interface{}
					err = json.Unmarshal([]byte(msg.Values["params"].(string)), &arr)
					if err != nil {
						if msg.Values["params"].(string) == "null" {
							arr = []interface{}{}
						} else {
							panic(err)
						}
					}

					var taskMq = taskMQ{
						TaskName:   msg.Values["task_name"].(string),
						SubmitTime: msg.Values["submit_time"].(string),
						RunTime:    msg.Values["run_time"].(string),
						Params:     arr,
					}

					taskFuncAddr, ok := task.taskFuncMap[taskMq.TaskName]
					if !ok {
						subTime, _ := util.Str2Int[int64](taskMq.SubmitTime)
						if subTime+task.taskEffectiveTime > time.Now().Unix() {
							execTime := time.Now().Unix() + 2
							task.addTaskMap(map[string]interface{}{
								"task_name":   taskMq.TaskName,
								"submit_time": msg.Values["submit_time"].(string),
								"run_time":    util.IntToStr(execTime),
								"params":      taskMq.Params,
							}, float64(execTime))
							log.Printf("task name[%s] taskfunc is not exists, but submit_time "+
								"fulfill a request so readd", taskMq.TaskName)
						}

						_, err = cli.XAck(ctx, val.Stream, task.groupName, msg.ID).Result()
						if err != nil {
							log.Println("ack ", msg.ID, ", err ", err)
						}
						_, err = cli.XDel(ctx, val.Stream, msg.ID).Result()
						if err != nil {
							log.Println("del ", msg.ID, ", err ", err)
						}
						log.Printf("msg id [%s] task name taskfunc is not exists", msg.ID)
						continue
					}

					task.threadPool.SubmitJob(&workpool.JobBag{
						JobFunc: task.innerTaskFuncBag,
						Params: []interface{}{
							msg.ID,
							val.Stream,
							taskFuncAddr,
							taskMq.Params,
							taskMq.TaskName,
						},
					})
				}
			}
		}

		for {
			innerTask()
			time.Sleep(200 * time.Millisecond)
		}
	}
	go innerFunc(t, &redis8.XReadGroupArgs{
		Streams:  []string{t.streamKey, ">"},
		Consumer: t.consumerName,
		Group:    t.groupName,
		Count:    1,
		// Block: 1 * time.Second,
		// NoAck: true,
	})
}

func (t *manager) checkParams() error {
	if t.taskMainKey == "" {
		return fmt.Errorf("taskMainKey is not blank")
	}

	if t.taskLabel == "" {
		return fmt.Errorf("taskLabel is not blank")
	}

	if t.calculateIntervalMS < 500 {
		t.calculateIntervalMS = 500
	}

	if t.redis == nil {
		return fmt.Errorf("redis is not blank")
	}

	if t.threadSize < 10 {
		t.threadSize = 10
	}

	return nil
}

func (t *manager) registerScheduledTaskActive() {
	innerFunc := func(t *manager) {
		defer func() {
			if p := recover(); p != nil {
				log.Println("registerScheduledTaskActive error")
				t.isTimerServer = false
			}
		}()

		if t.redis.Register(t.taskMainKey, t.serverUUID, 5) {
			t.isTimerServer = true
		} else {
			t.isTimerServer = false
		}
	}

	go func() {
		for {
			innerFunc(t)
			time.Sleep(time.Duration(t.calculateIntervalMS) * time.Millisecond)
		}
	}()
}

func (t *manager) RegisterTask(taskName string, fun funcType) {
	if taskName == "" {
		log.Fatalf("task name not be blank")
		return
	}

	if _, ok := t.taskFuncMap[taskName]; ok {
		log.Fatalf("task: [%s] is already exists", taskName)
		return
	}

	t.taskFuncMap[taskName] = fun
}

func (t *manager) RegisterHighLevelTask(taskName string, fun interface{}) {
	if taskName == "" {
		log.Fatalf("task name not be blank")
		return
	}

	if _, ok := t.taskFuncMap[taskName]; ok {
		log.Fatalf("task: [%s] is already exists", taskName)
		return
	}

	m := taskfunc.NewAsyncFuncType(fun)
	t.taskFuncMap[taskName] = m
}

func (t *manager) AddSimpleTask(taskName string, args []interface{}) {
	t.addSimpleTask(taskName, args)
}

func (t *manager) addSimpleTask(taskName string, args []interface{}) {
	t.addTaskList(&funcData{
		taskName:       taskName,
		args:           args,
		runTime:        time.Now().Unix(),
		taskType:       "simple", // delay、interval、timer、simple
		timer:          nil,
		intervalSecond: 0,
	})
}

// AddDelayTask
// 异步任务管理器，要点：
// 1、所有任务必须注册才可调用
// 2、任务参数必须可序列化、反序列化
// 3、任务管理器只保证任务执行，业务逻辑报错需要业务处理
func (t *manager) AddDelayTask(taskName string, args []interface{}, delayTime uint) {
	t.addDelayTask(taskName, args, delayTime)
}

func (t *manager) addDelayTask(taskName string, args []interface{}, delayTime uint) {
	t.addTaskList(&funcData{
		taskName:       taskName,
		args:           args,
		runTime:        time.Now().Unix() + int64(delayTime),
		taskType:       "delay", // delay、interval、timer、simple
		timer:          nil,
		intervalSecond: 0,
	})
}

// AddIntervalTask
// 异步任务管理器，要点：
// 1、所有任务必须注册才可调用
// 2、任务参数必须可序列化、反序列化
// 3、任务管理器只保证任务执行，业务逻辑报错需要业务处理
func (t *manager) AddIntervalTask(taskName string, args []interface{}, interval int) {
	t.addIntervalTask(taskName, args, interval)
}

func (t *manager) addIntervalTask(taskName string, args []interface{}, interval int) {
	t.addTaskList(&funcData{
		taskName:       taskName,
		args:           args,
		runTime:        time.Now().Unix(),
		taskType:       "interval", // delay、interval、timer、simple
		timer:          nil,
		intervalSecond: interval,
	})
}

func (t *manager) addTaskList(task *funcData) bool {
	if task.taskName == "" {
		panic("task name not be blank")
	}

	if _, ok := t.taskFuncMap[task.taskName]; !ok {
		panic(fmt.Sprintf("task:[%s] not be registered", task.taskName))
	}

	if task.args == nil {
		task.args = []interface{}{}
	}

	if task.taskType == "simple" {
		t.addTaskStruct(&taskMQ{
			TaskName:   task.taskName,
			SubmitTime: util.IntToStr(time.Now().Unix()),
			RunTime:    util.IntToStr(time.Now().Unix()),
			Params:     task.args,
		})
	} else if task.taskType == "delay" {
		t.addTaskMap(map[string]interface{}{
			"task_name":   task.taskName,
			"submit_time": util.IntToStr(time.Now().Unix()),
			"run_time":    util.IntToStr(task.runTime),
			"params":      task.args,
		}, float64(task.runTime))
	} else {
		t.taskQueue = append(t.taskQueue, task)
	}
	return true
}

func (t *manager) addTimerTask(taskName string, args []interface{}, timer []int) bool {
	nextTime := t.calculateNextRunTime(timer, nil, true)
	if nextTime < 0 {
		return false
	}

	t.addTaskList(&funcData{
		taskName:       taskName,
		args:           args,
		runTime:        nextTime,
		taskType:       "timer", // delay、interval、timer、simple
		timer:          timer,
		intervalSecond: 0,
	})

	return true
}

// AddTimerTask
// 异步任务管理器，要点：
// 1、所有任务必须注册才可调用
// 2、任务参数必须可序列化、反序列化
// 3、任务管理器只保证任务执行，业务逻辑报错需要业务处理
func (t *manager) AddTimerTask(taskName string, args []interface{}, timer Timer) {
	timeParam := timer.timerParams()
	if len(timeParam) <= 0 {
		panic("AddTimerTask [month, day, hour, minutes, second] can't be all none")
	}

	t.addTimerTask(taskName, args, timeParam)
}

func (t *manager) getNextTime(b string, minTime time.Time, p map[string]int) *time.Time {
	switch b {
	case "minute":
		p["year"] = minTime.Year()
		p["month"] = int(minTime.Month())
		p["day"] = minTime.Day()
		p["hour"] = minTime.Hour()
		p["minute"] = minTime.Minute()

		nt, _ := str2Time(initMyTime(p).string())
		d, _ := time.ParseDuration("1m")
		for {
			if minTime.Unix() < nt.Unix() {
				break
			}
			nt = nt.Add(d)
		}
		return &nt
	case "hour":
		p["year"] = minTime.Year()
		p["month"] = int(minTime.Month())
		p["day"] = minTime.Day()
		p["hour"] = minTime.Hour()

		nt, _ := str2Time(initMyTime(p).string())
		d, _ := time.ParseDuration("1h")
		for {
			if minTime.Unix() < nt.Unix() {
				break
			}
			nt = nt.Add(d)
		}
		return &nt
	case "day":
		p["year"] = minTime.Year()
		p["month"] = int(minTime.Month())
		p["day"] = minTime.Day()

		nt, _ := str2Time(initMyTime(p).string())
		d, _ := time.ParseDuration("24h")
		for {
			if minTime.Unix() < nt.Unix() {
				break
			}
			nt = nt.Add(d)
		}
		return &nt
	case "month":
		p["year"] = minTime.Year()
		p["month"] = int(minTime.Month())

		for {
			nt, err := str2Time(initMyTime(p).string())
			if err != nil || minTime.Unix() >= nt.Unix() {
				m := p["month"]
				if m == 12 {
					p["month"] = 1
					p["year"]++
				} else {
					p["month"] = m + 1
				}
				continue
			}
			return &nt
		}
	case "year":
		p["year"] = minTime.Year()

		for {
			nt, err := str2Time(initMyTime(p).string())
			if err != nil || minTime.Unix() >= nt.Unix() {
				p["year"]++
				continue
			}
			return &nt
		}
	default:
		return nil
	}
}

// timer: [second, minute, hour, day, month]
// currentTime: 当前比对时间
// check: 是否检查参数
func (t *manager) calculateNextRunTime(timer []int, currentTime *time.Time, check bool) int64 {
	sz := len(timer)
	if sz <= 0 {
		log.Printf("time conf is nil")
		panic("time conf is nil")
	}

	if check {
		for i := 0; i < sz; i++ {
			conf := timeConf[i]
			if timer[i] < conf[1].(int) || timer[i] > conf[2].(int) {
				errStr := fmt.Sprintf("%s between %d and %d", conf[0], conf[1], conf[2])
				log.Println(errStr)
				panic(errStr)
			}
			if conf[0].(string) == "month" {
				if timer[i-1] > monthDays[timer[i]-1] {
					errStr := fmt.Sprintf("%s between %d and %d", conf[0], conf[1], conf[2])
					log.Println(errStr)
					panic(errStr)
				}
			}
		}
	}

	now := time.Now()
	if currentTime != nil {
		now = *currentTime
	}

	param := map[string]int{
		"month":  0,
		"day":    0,
		"hour":   0,
		"minute": 0,
		"second": 0,
	}
	baseUnit := ""

	if sz >= 1 {
		param["second"] = timer[0]
	}

	if sz >= 2 {
		param["minute"] = timer[1]
	} else {
		baseUnit = "minute"
	}

	if sz >= 3 {
		param["hour"] = timer[2]
	} else {
		if baseUnit == "" {
			baseUnit = "hour"
		}
	}

	if sz >= 4 {
		param["day"] = timer[3]
	} else {
		if baseUnit == "" {
			baseUnit = "day"
		}
	}

	if sz >= 5 {
		param["month"] = timer[4]
	} else {
		if baseUnit == "" {
			baseUnit = "month"
		}
	}

	if baseUnit == "" {
		baseUnit = "year"
	}

	nextRunTime := t.getNextTime(baseUnit, now, param)
	if nextRunTime != nil {
		return nextRunTime.Unix()
	}
	return -1
}

func (t *manager) clearStreamData() {
	innerFunc := func(task *manager) {
		cli, ctx := task.redis.Raw()
		for {
			msgList, err := cli.XPendingExt(ctx, &redis8.XPendingExtArgs{
				Stream: task.streamKey,
				Group:  task.groupName,
				Start:  "-",
				End:    "+",
				Count:  100,
			}).Result()
			if err != nil {
				log.Println(err)
				return
			}

			var msgIDs []string
			for _, msg := range msgList {
				if msg.Idle >= msgMaxIdleTime {
					msgIDs = append(msgIDs, msg.ID)
				} else {
					break
				}
			}

			if len(msgIDs) <= 0 {
				return
			}

			cli.XAck(ctx, task.streamKey, task.groupName, msgIDs...)
			cli.XDel(ctx, task.streamKey, msgIDs...)
		}
	}

	go func() {
		for {
			innerFunc(t)
			time.Sleep(time.Second * 10)
		}
	}()
}

func (t *manager) RunTaskManager() {
	t.threadPool = workpool.NewWorkPool(t.threadSize, "task_manager_pool_"+t.taskLabel, 0, t.threadSize)

	t.registerScheduledTaskActive()
	t.initDemo()
	t.initRedisData()
	t.clearStreamData()
	t.taskWatch()
	t.computeTask()
	t.computeDelayTask()
}

// NewManager 创建任务管理器
func NewManager(workPoolSize int, taskLabel string, calculateIntervalMS int64, redisConn *redis.Redis) Manager {
	task := new(manager)

	uuidV4, err := uuid.NewV4()
	if err != nil {
		log.Println(err)
		panic(err)
	}

	task.serverUUID = uuidV4.String()

	task.taskEffectiveTime = 60
	task.calculateIntervalMS = calculateIntervalMS
	task.taskLabel = taskLabel
	task.taskMainKey = "go_task_manager_" + task.taskLabel
	task.threadSize = workPoolSize
	task.redis = redisConn

	task.streamKey = "task_stream_mq_" + task.taskLabel
	task.groupName = "task_group_" + task.taskLabel
	task.consumerName = "task_consumer_" + task.taskLabel

	task.delayZSetKey = "task_delay_sort_set_" + task.taskLabel

	task.taskFuncMap = map[string]interface{}{}
	task.taskSet = map[string]struct{}{}

	err = task.checkParams()
	if err != nil {
		panic(err)
	}
	return task
}

// SingleTask 创建TaskManager单例
func SingleTask(workPoolSize int, taskLabel string, calculateIntervalMS int64, redisConn *redis.Redis) Manager {
	if globalManager == nil {
		taskLock.Lock()
		defer taskLock.Unlock()

		if globalManager == nil {
			globalManager = NewManager(workPoolSize, taskLabel, calculateIntervalMS, redisConn)
		}
	}

	return globalManager
}
