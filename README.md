# task

包含：

## 1、task manager
> 异步任务管理器：可以处理异步任务、延迟任务、定时任务、间隔任务，满足日常的研发需求，提高研发效率

使用示例
```go
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
```

## 2、task executor
> 并发任务执行器，可以将多个任务进行并发处理，提高资源利用率，提高系统吞吐量

使用示例
```go
import (
	"context"
	"fmt"
	"strconv"
	"testing"
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

func hello3() (*int, error) {
	fmt.Println("hello,")
	// time.Sleep(time.Second * 1)
	// r := i[0]
	// panic(111)
	return nil, nil
}

func TestTaskExecuteSimple(t *testing.T) {
	taskObj := NewExecutor("test111")
	taskObj.AddFixed(hello1, 0)
	taskObj.AddFlexible(func(i int) (int64, error) {
		fmt.Println("test: ", i)
		return 1, nil
	}, 1)
	taskObj.AddFlexible(hello3)
	r, taskErr, err := taskObj.ExecuteWithErr(context.Background())
	if err != nil {
		panic(err)
	}

	fmt.Println(taskErr)

	for i, v := range r {
		if v == nil {
			fmt.Println(strconv.Itoa(i) + ": nil")
		} else {
			fmt.Printf("%d: %v\n", i, v)
		}
	}
}
```

## 3、task queue
> 队列任务执行器，可以将数据生产与消费独立控制，提高资源利用率，提高系统吞吐量

使用示例（并发队列，不保证有序，性能高）
```go
func TestNewQueue(t *testing.T) {
	q := NewQueue(context.Background(), 1, 0)
	q.ConsumeFlexible(func(i int) {
		fmt.Println(i)
	})

	for i := 1; i < 100; i++ {
		q.AddMsg(i)
	}

    q.WaitFinish()
}
```

使用示例（有序队列，保证有序，性能低于并发队列）
```go
func TestNewSortedQueue(t *testing.T) {
    q := NewSortedQueue(context.Background(), 0)
    q.ConsumeFlexible(func(i int) {
        fmt.Println(i)
    })

    for i := 1; i < 100; i++ {
        q.AddMsg(i)
    }

    q.WaitFinish()
}
```