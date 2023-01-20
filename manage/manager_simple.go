package manage

import (
	"fmt"
	"time"

	"github.com/assembly-hub/basics/redis"
)

func SimpleTask() {
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

	for {
		time.Sleep(5 * time.Second)
	}
}
