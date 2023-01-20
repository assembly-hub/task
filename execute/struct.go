// Package execute
package execute

import "github.com/assembly-hub/task/taskfunc"

type executorFunc func(param ...interface{}) (interface{}, error)

type taskFunc struct {
	f executorFunc
	m *taskfunc.ConcurrencyFuncType
	p []interface{}
}
