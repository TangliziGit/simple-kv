package engines

import (
	"sync"
	"time"
)

type Thread struct {
	TaskChan chan func() bool
	Group    *sync.WaitGroup
}

func NewThread(group *sync.WaitGroup) *Thread {
	thread := &Thread{
		TaskChan: make(chan func() bool, 100),
		Group:    group,
	}
	return thread
}

func (t *Thread) Run() *Thread {
	go func() {
		for fun := range t.TaskChan {
			exit := fun()
			if exit {
				t.Group.Done()
				return
			}
		}
	}()
	return t
}

func (t *Thread) Do(fun func() bool) {
	time.Sleep(time.Millisecond)
	t.TaskChan <- fun
}
