// Package taskfunc
package taskfunc

import (
	"context"
	"reflect"

	"github.com/assembly-hub/basics/util"
	"github.com/assembly-hub/log"
)

type ConcurrencyFuncType struct {
	ctx          context.Context
	funcType     reflect.Type
	funcValType  reflect.Value
	paramsType   []reflect.Type
	isVariadic   bool
	variadicType reflect.Type
	logger       log.Log
}

func NewConcurrencyFuncType(ctx context.Context, f interface{}, logger log.Log) *ConcurrencyFuncType {
	tf := new(ConcurrencyFuncType)
	tf.logger = logger
	tf.ctx = ctx
	tf.funcType = reflect.TypeOf(f)
	if tf.funcType.Kind() != reflect.Func {
		panic("NewConcurrencyFuncType param must be taskfunc(param ...interface{}) (any, error) or " +
			"taskfunc(i int, s string, ...) (any, error)")
	}
	if tf.funcType.NumOut() != 2 || tf.funcType.Out(1).String() != "error" {
		panic("task taskfunc return type must be (any, error)")
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
			err := util.Any2Any(param[i], v)
			if err != nil {
				tf.logger.Error(tf.ctx, err.Error())
				return nil, err
			}
			callParam = append(callParam, reflect.ValueOf(v).Elem())
		}
		if len(param) > needParamLen {
			for i, n := needParamLen, paramLen; i < n; i++ {
				v := reflect.New(tf.variadicType).Interface()
				err := util.Any2Any(param[i], v)
				if err != nil {
					tf.logger.Error(tf.ctx, err.Error())
					return nil, err
				}
				callParam = append(callParam, reflect.ValueOf(v).Elem())
			}
		}
	} else {
		for i, n := 0, len(tf.paramsType); i < n; i++ {
			v := reflect.New(tf.paramsType[i]).Interface()
			err := util.Any2Any(param[i], v)
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
	var val interface{}
	if ret[0].Kind() == reflect.Pointer && ret[0].IsNil() {
		val = nil
	} else {
		val = ret[0].Interface()
	}
	return val, err
}

type AsyncFuncType struct {
	ctx          context.Context
	funcType     reflect.Type
	funcValType  reflect.Value
	paramsType   []reflect.Type
	isVariadic   bool
	variadicType reflect.Type
	logger       log.Log
}

func NewAsyncFuncType(ctx context.Context, f interface{}, logger log.Log) *AsyncFuncType {
	tf := new(AsyncFuncType)
	tf.logger = logger
	tf.ctx = ctx
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
			err := util.Any2Any(param[i], v)
			if err != nil {
				tf.logger.Error(tf.ctx, err.Error())
				return
			}
			callParam = append(callParam, reflect.ValueOf(v).Elem())
		}
		if len(param) > needParamLen {
			for i, n := needParamLen, paramLen; i < n; i++ {
				v := reflect.New(tf.variadicType).Interface()
				err := util.Any2Any(param[i], v)
				if err != nil {
					tf.logger.Error(tf.ctx, err.Error())
					return
				}
				callParam = append(callParam, reflect.ValueOf(v).Elem())
			}
		}
	} else {
		for i, n := 0, len(tf.paramsType); i < n; i++ {
			v := reflect.New(tf.paramsType[i]).Interface()
			err := util.Any2Any(param[i], v)
			if err != nil {
				tf.logger.Error(tf.ctx, err.Error())
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
