package queue

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestNewQueue(t *testing.T) {
	q := NewQueue(context.Background(), 1, 0)
	q.ConsumeFlexible(func(i int) {
		fmt.Println(i)
	})

	for i := 1; i < 100; i++ {
		q.AddMsg(i)
	}

	time.Sleep(time.Second)
}

func TestNewQueue2(t *testing.T) {
	q := NewQueue(context.Background(), 10, 90)
	q.ConsumeFlexible(func(i int) {
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

func TestNewQueue3(t *testing.T) {
	q := NewQueue(context.Background(), 10, 90)
	q.ConsumeFlexible(func(i int) {
		fmt.Println("------------", i)
	})

	for i := 0; i < 100; i++ {
		q.AddMsg(i)
	}

	q.WaitFinish()
	fmt.Println("finished")
}
