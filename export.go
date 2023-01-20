// Package task
package task

import (
	"github.com/assembly-hub/task/execute"
	"github.com/assembly-hub/task/manage"
)

type Executor = execute.Executor

var NewTaskExecutor = execute.NewExecutor

var SingleTask = manage.SingleTask

type Manager = manage.Manager

var NewTimer = manage.NewTimer

type Timer = manage.Timer
