package queue

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestNewSortedQueue(t *testing.T) {
	q := NewSortedQueue(context.Background(), 0)
	q.ConsumeFlexible(func(i int) {
		fmt.Println(i)
	})

	for i := 1; i < 100; i++ {
		q.AddMsg(i)
	}

	time.Sleep(time.Second)
}

func TestNewSortedQueue2(t *testing.T) {
	q := NewSortedQueue(context.Background(), 100)
	q.ConsumeFixed(func(i ...any) {
		fmt.Println(i)
	})

	for i := 1; i < 100; i++ {
		q.AddMsg(i)
	}

	for {
		if q.IsFinished() {
			break
		}
		time.Sleep(time.Second)
	}
}

func TestNewSortedQueue3(t *testing.T) {
	q := NewSortedQueue(context.Background(), 100)
	q.ConsumeFixed(func(i ...any) {
		fmt.Println("------", i)
		time.Sleep(time.Millisecond * 10)
	})

	for i := 0; i < 100; i++ {
		q.AddMsg(i)
	}

	q.WaitFinish()
	fmt.Println("finished")
}
