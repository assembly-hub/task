package manage

import (
	"fmt"
	"testing"
)

func TestNewManager(t *testing.T) {
	SimpleTask()
}

func TestNewManager2(t *testing.T) {
	SimpleTask()
}

func TestNewTimer(t *testing.T) {
	tm := NewTimer()
	tm.SetDay(1)
	tm.SetMinutes(1)
	fmt.Println(tm.timerParams())
}
