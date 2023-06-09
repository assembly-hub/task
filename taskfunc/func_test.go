package taskfunc

import (
	"context"
	"fmt"
	"testing"

	"github.com/assembly-hub/basics/util"
	"github.com/assembly-hub/log/empty"
)

func MyFunc(s string, i float64) (int, error) {
	fmt.Println(s, i)
	return 0, fmt.Errorf("test")
}

func MyFunc2(s string, i float64) (interface{}, error) {
	fmt.Println(s, i)
	return 0, fmt.Errorf("test")
}

func TestNewTaskFuncType(t *testing.T) {
	tf := NewConcurrencyFuncType(context.Background(), MyFunc2, empty.NoLog)
	ret, err := tf.Call("1", float64(1))
	fmt.Println(ret, err)
}

func MyFunc3(p ...interface{}) (interface{}, error) {
	fmt.Println(p...)
	return 0, fmt.Errorf("test")
}

func TestNewTaskFuncType2(t *testing.T) {
	tf := NewConcurrencyFuncType(context.Background(), MyFunc3, empty.NoLog)
	ret, err := tf.Call("1", float64(1))
	fmt.Println(ret, err)
}

type TestStruct struct {
	Test string `json:"test"`
}

func MyFunc5(i int, s string, st TestStruct, ii ...int) (interface{}, error) {
	fmt.Println(i, s, st, ii)
	return 0, fmt.Errorf("test")
}

func TestNewTaskCall(t *testing.T) {
	tf := NewConcurrencyFuncType(context.Background(), MyFunc5, empty.NoLog)
	ret, err := tf.Call(1, "2", TestStruct{
		Test: "test",
	}, 1, 2, 3)
	fmt.Println(ret, err)
}

func MyFunc6(i int, s string, st TestStruct, ii ...float64) (interface{}, error) {
	fmt.Println(i, s, st, ii)
	return 0, fmt.Errorf("test")
}

func TestNewFormatCall(t *testing.T) {
	valList := []interface{}{
		1, "str", TestStruct{
			Test: "test",
		}, 1, 2, 3,
	}
	err := util.Any2Any(valList, &valList)
	if err != nil {
		panic(err)
	}
	tf := NewConcurrencyFuncType(context.Background(), MyFunc6, empty.NoLog)
	ret, err := tf.FormatParamCall(valList...)
	fmt.Println(ret, err)
}

func Func8() (int, error) {
	i := 0
	return i, nil
}

func TestNewFormatCall2(t *testing.T) {
	tf := NewConcurrencyFuncType(context.Background(), Func8, empty.NoLog)
	ret, err := tf.FormatParamCall()
	fmt.Println(ret, err)
}
