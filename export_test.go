package task

import (
	"fmt"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/assembly-hub/basics/redis"
)

func hello1(i ...interface{}) (interface{}, error) {
	fmt.Printf("hello,欢迎来到编程狮%v\n", i)
	// time.Sleep(time.Second * 1)
	// panic(123)
	return i[0], nil
}

func hello2(i ...interface{}) (interface{}, error) {
	fmt.Printf("hello,欢迎来到编程狮%v\n", i)
	// time.Sleep(time.Second * 1)
	// r := i[0]
	// panic(111)
	return i[0], nil
}

func TestExecute(t *testing.T) {
	taskObj := NewTaskExecutor("test111")
	taskObj.AddTask(hello1, 0)
	taskObj.AddSimpleTask(func(i int) (interface{}, error) {
		fmt.Println("test: ", i)
		return nil, nil
	}, 1)
	r, taskErr, err := taskObj.ExecuteTaskWithErr()
	if err != nil {
		panic(err)
	}

	fmt.Println(taskErr)

	for i, v := range r {
		if v == nil {
			fmt.Println(strconv.Itoa(i) + ": nil")
		} else {
			fmt.Println(strconv.Itoa(i) + ": " + strconv.Itoa(v.(int)))
		}
	}
}

func TestManege(t *testing.T) {
	defer func() {
		if p := recover(); p != nil {
			log.Println(p)
		}
	}()

	opts := redis.DefaultOptions()
	opts.Addr = "127.0.0.1:6379"
	opts.DB = 0

	r := redis.NewRedis(&opts)

	// step 1, init task manager
	task := SingleTask(100, "test", 1500, r)

	// step 1.1 非必须，默认时间为：60秒
	task.SetNoTaskEffectiveTime(10)

	// step 2, register task
	task.RegisterHighLevelTask("test_111", func(i int, s string, arr []int, mp map[string]interface{}) {
		fmt.Println("-------task_111 params: ", i, s, arr, mp)
	})
	task.RegisterHighLevelTask("test_222", func() {
		fmt.Println("-------task_222 ")
	})
	// step 3， run task manager
	task.RunTaskManager()

	// step 4 use task

	// add Interval Task
	task.AddIntervalTask("test_111", []interface{}{1, "2", []int{1, 2, 3}, map[string]interface{}{
		"test": "test", "key": []string{"test"},
	}}, 5)

	task.AddIntervalTask("test_222", nil, 5)

	task.AddDelayTask("test_222", nil, 2)

	for {
		time.Sleep(5 * time.Second)
		break
	}
}
