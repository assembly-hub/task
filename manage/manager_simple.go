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
	task.SetTaskEffectiveTime(10)

	// step 2, register task
	task.RegisterFlexible("test_111", func(i int, s string, arr []int, mp map[string]interface{}) {
		// fmt.Println("-------task_111 params: ", i, s, arr, mp)
	})
	task.RegisterFlexible("test_222", func() {
		// fmt.Println("-------task_222 ")
	})
	task.RegisterFlexible("test_333", func() {
		fmt.Println("-------task_333 ")
		// time.Sleep(time.Second * 1000)
	})
	// step 3， run task manager
	task.Run()

	// step 4 use task

	// add Interval Task
	//task.AddInterval("test_111", []interface{}{1, "2", []int{1, 2, 3}, map[string]interface{}{
	//	"test": "test", "key": []string{"test"},
	//}}, 5)
	//
	// task.AddInterval("test_222", nil, 5)

	task.AddInterval("test_333", nil, 5)
	// task.AddSimple("test_333", nil)

	for {
		time.Sleep(5 * time.Second)
	}
}
