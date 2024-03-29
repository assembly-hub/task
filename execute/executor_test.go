package execute

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
