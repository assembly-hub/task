package taskfunc

import (
	"fmt"
	"testing"

	"github.com/assembly-hub/basics/util"
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
	tf := NewConcurrencyFuncType(MyFunc2)
	ret, err := tf.Call("1", float64(1))
	fmt.Println(ret, err)
}

func MyFunc3(p ...interface{}) (interface{}, error) {
	fmt.Println(p...)
	return 0, fmt.Errorf("test")
}

func TestNewTaskFuncType2(t *testing.T) {
	tf := NewConcurrencyFuncType(MyFunc3)
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
	tf := NewConcurrencyFuncType(MyFunc5)
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
	err := util.Interface2Interface(valList, &valList)
	if err != nil {
		panic(err)
	}
	tf := NewConcurrencyFuncType(MyFunc6)
	ret, err := tf.FormatParamCall(valList...)
	fmt.Println(ret, err)
}
