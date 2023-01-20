package execute

import (
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

func TestTaskExecuteSimple(t *testing.T) {
	taskObj := NewExecutor("test111")
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
