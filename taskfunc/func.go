// Package taskfunc
package taskfunc

import (
	"log"
	"reflect"

	"github.com/assembly-hub/basics/util"
)

type ConcurrencyFuncType struct {
	funcType     reflect.Type
	funcValType  reflect.Value
	paramsType   []reflect.Type
	isVariadic   bool
	variadicType reflect.Type
}

func NewConcurrencyFuncType(f interface{}) *ConcurrencyFuncType {
	tf := new(ConcurrencyFuncType)
	tf.funcType = reflect.TypeOf(f)
	if tf.funcType.Kind() != reflect.Func {
		panic("NewConcurrencyFuncType param must be taskfunc(param ...interface{}) (interface{}, error) or " +
			"taskfunc(i int, s string, ...) (interface{}, error)")
	}
	if tf.funcType.NumOut() != 2 || tf.funcType.Out(0).Kind() != reflect.Interface || tf.funcType.Out(1).String() != "error" {
		panic("task taskfunc return type must be (interface{}, error)")
	}

	tf.funcValType = reflect.ValueOf(f)
	for i := 0; i < tf.funcType.NumIn(); i++ {
		tf.paramsType = append(tf.paramsType, tf.funcType.In(i))
	}
	tf.isVariadic = tf.funcType.IsVariadic()
	if tf.isVariadic {
		tf.variadicType = tf.paramsType[len(tf.paramsType)-1].Elem()
	}
	return tf
}

func (tf *ConcurrencyFuncType) FormatParamCall(param ...interface{}) (interface{}, error) {
	var callParam []reflect.Value
	if tf.isVariadic {
		paramLen := len(param)
		needParamLen := len(tf.paramsType) - 1
		for i, n := 0, needParamLen; i < n; i++ {
			v := reflect.New(tf.paramsType[i]).Interface()
			err := util.Interface2Interface(param[i], v)
			if err != nil {
				return nil, err
			}
			callParam = append(callParam, reflect.ValueOf(v).Elem())
		}
		if len(param) > needParamLen {
			for i, n := needParamLen, paramLen; i < n; i++ {
				v := reflect.New(tf.variadicType).Interface()
				err := util.Interface2Interface(param[i], v)
				if err != nil {
					return nil, err
				}
				callParam = append(callParam, reflect.ValueOf(v).Elem())
			}
		}
	} else {
		for i, n := 0, len(tf.paramsType); i < n; i++ {
			v := reflect.New(tf.paramsType[i]).Interface()
			err := util.Interface2Interface(param[i], v)
			if err != nil {
				return nil, err
			}
			callParam = append(callParam, reflect.ValueOf(v).Elem())
		}
	}
	return tf.call(callParam)
}

func (tf *ConcurrencyFuncType) Call(param ...interface{}) (interface{}, error) {
	var callParam []reflect.Value
	for _, p := range param {
		callParam = append(callParam, reflect.ValueOf(p))
	}
	return tf.call(callParam)
}

func (tf *ConcurrencyFuncType) call(callParam []reflect.Value) (interface{}, error) {
	ret := tf.funcValType.Call(callParam)
	var err error
	if ret[1].Interface() != nil {
		err = ret[1].Interface().(error)
	}
	return ret[0].Interface(), err
}

type AsyncFuncType struct {
	funcType     reflect.Type
	funcValType  reflect.Value
	paramsType   []reflect.Type
	isVariadic   bool
	variadicType reflect.Type
}

func NewAsyncFuncType(f interface{}) *AsyncFuncType {
	tf := new(AsyncFuncType)
	tf.funcType = reflect.TypeOf(f)
	if tf.funcType.Kind() != reflect.Func {
		panic("NewConcurrencyFuncType param must be taskfunc(param ...interface{}) or taskfunc(i int, s string, ...)")
	}
	if tf.funcType.NumOut() != 0 {
		panic("task taskfunc need not return any data")
	}

	tf.funcValType = reflect.ValueOf(f)
	for i := 0; i < tf.funcType.NumIn(); i++ {
		tf.paramsType = append(tf.paramsType, tf.funcType.In(i))
	}
	tf.isVariadic = tf.funcType.IsVariadic()
	if tf.isVariadic {
		tf.variadicType = tf.paramsType[len(tf.paramsType)-1].Elem()
	}
	return tf
}

func (tf *AsyncFuncType) FormatParamCall(param ...interface{}) {
	var callParam []reflect.Value
	if tf.isVariadic {
		paramLen := len(param)
		needParamLen := len(tf.paramsType) - 1
		for i, n := 0, needParamLen; i < n; i++ {
			v := reflect.New(tf.paramsType[i]).Interface()
			err := util.Interface2Interface(param[i], v)
			if err != nil {
				log.Println(err)
				return
			}
			callParam = append(callParam, reflect.ValueOf(v).Elem())
		}
		if len(param) > needParamLen {
			for i, n := needParamLen, paramLen; i < n; i++ {
				v := reflect.New(tf.variadicType).Interface()
				err := util.Interface2Interface(param[i], v)
				if err != nil {
					log.Println(err)
					return
				}
				callParam = append(callParam, reflect.ValueOf(v).Elem())
			}
		}
	} else {
		for i, n := 0, len(tf.paramsType); i < n; i++ {
			v := reflect.New(tf.paramsType[i]).Interface()
			err := util.Interface2Interface(param[i], v)
			if err != nil {
				log.Println(err)
				return
			}
			callParam = append(callParam, reflect.ValueOf(v).Elem())
		}
	}
	tf.call(callParam)
}

func (tf *AsyncFuncType) Call(param ...interface{}) {
	var callParam []reflect.Value
	for _, p := range param {
		callParam = append(callParam, reflect.ValueOf(p))
	}
	tf.call(callParam)
}

func (tf *AsyncFuncType) call(callParam []reflect.Value) {
	_ = tf.funcValType.Call(callParam)
}
