// Package manage
package manage

import (
	"fmt"
	"time"
)

// Timer task timer config data
type Timer struct {
	Month   *int
	Day     *int
	Hour    *int
	Minutes *int
	Second  *int
}

func str2Time(timeStr string) (time.Time, error) {
	t, err := time.ParseInLocation("2006-01-02 15:04:05", timeStr, time.Local)
	return t, err
}

func NewTimer() *Timer {
	t := new(Timer)
	return t
}

func (t *Timer) SetMonth(i int) *Timer {
	t.Month = &i
	return t
}

func (t *Timer) SetDay(i int) *Timer {
	t.Day = &i
	return t
}

func (t *Timer) SetHour(i int) *Timer {
	t.Hour = &i
	return t
}

func (t *Timer) SetMinutes(i int) *Timer {
	t.Minutes = &i
	return t
}

func (t *Timer) SetSecond(i int) *Timer {
	t.Second = &i
	return t
}

func (t *Timer) timerParams() []int {
	var params []int
	var isValid = false

	if t.Month != nil {
		isValid = true
		params = append(params, *t.Month)
	}

	if t.Day == nil {
		if isValid {
			params = append(params, 1)
		}
	} else {
		isValid = true
		params = append(params, *t.Day)
	}

	if t.Hour == nil {
		if isValid {
			params = append(params, 0)
		}
	} else {
		isValid = true
		params = append(params, *t.Hour)
	}

	if t.Minutes == nil {
		if isValid {
			params = append(params, 0)
		}
	} else {
		isValid = true
		params = append(params, *t.Minutes)
	}

	if t.Second == nil {
		if isValid {
			params = append(params, 0)
		}
	} else {
		params = append(params, *t.Second)
	}

	if params == nil || len(params) <= 0 {
		return nil
	}

	var temp int
	arrLen := len(params)
	for i, j := 0, arrLen-1; i < j; {
		temp = params[i]
		params[i] = params[j]
		params[j] = temp

		i++
		j--
	}

	return params
}

// 调度时间数据
type myTime struct {
	Year    int
	Month   int
	Day     int
	Hour    int
	Minutes int
	Second  int
}

func initMyTime(m map[string]int) *myTime {
	t := new(myTime)
	t.Year = m["year"]
	t.Month = m["month"]
	t.Day = m["day"]
	t.Hour = m["hour"]
	t.Minutes = m["minute"]
	t.Second = m["second"]
	return t
}

func (t *myTime) string() string {
	return fmt.Sprintf("%4d-%02d-%02d %02d:%02d:%02d", t.Year, t.Month, t.Day, t.Hour, t.Minutes, t.Second)
}

type funcType func(param ...interface{})

type funcData struct {
	taskName       string
	args           []interface{}
	runTime        int64
	taskType       string // delay、interval、timer、simple
	timer          []int  // timer
	intervalSecond int    // interval
}
