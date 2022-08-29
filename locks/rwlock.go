package locks

import (
	"fmt"
	"simple-kv/txns"
	"sync"
	"sync/atomic"
)

// TODO: need test

var taskCounter = uint64(0)

type Task struct {
	ID     uint64
	Txn    *txns.Txn
	IsRead bool

	Next *Task
}

func NewTask(txn *txns.Txn, isRead bool) *Task {
	return &Task{
		ID:     atomic.AddUint64(&taskCounter, 1),
		Txn:    txn,
		IsRead: isRead,
		Next:   nil,
	}
}

type Operator interface {
	ActiveLock(lock *RWLock)
	InactiveLock(lock *RWLock)
}

type RWLock struct {
	WaitingHead *Task
	WaitingTail *Task

	Condition     *sync.Cond
	WritingTxnID  uint64
	ReadingTxnIDs map[uint64]struct{}
	AllowTaskID   uint64

	Op    Operator
	Latch *sync.Mutex
}

func (l *RWLock) GetWritingTxnID() uint64 {
	return atomic.LoadUint64(&l.WritingTxnID)
}

func (l *RWLock) GetReadingTxnIDs() (m map[uint64]struct{}) {
	l.Latch.Lock()
	defer l.Latch.Unlock()

	m = map[uint64]struct{}{}
	for txnID := range l.ReadingTxnIDs {
		m[txnID] = struct{}{}
	}
	return
}

func (l *RWLock) PushTask(txn *txns.Txn, isRead bool) *Task {
	if l.WaitingTail == nil {
		l.WaitingHead = NewTask(txn, isRead)
		l.WaitingTail = l.WaitingHead
		return l.WaitingHead
	}

	task := NewTask(txn, isRead)
	l.WaitingTail.Next = task
	l.WaitingTail = l.WaitingTail.Next
	return task
}

func (l *RWLock) PopTask() *Task {
	if l.WaitingHead == nil {
		return nil
	}

	task := l.WaitingHead
	l.WaitingHead = l.WaitingHead.Next
	return task
}

func (l *RWLock) CancelTask(txn *txns.Txn) {
	if l.WaitingHead == nil {
		return
	}
	if l.WaitingHead.Txn == txn {
		l.WaitingHead.Txn = nil
		l.WaitingHead = l.WaitingHead.Next
		return
	}

	head := l.WaitingHead
	for head.Next != nil {
		if head.Next.Txn != txn {
			continue
		}

		head.Next.Txn = nil
		head.Next = head.Next.Next
		break
	}
}

func (l *RWLock) wait(txn *txns.Txn, isRead bool) error {
	task := l.PushTask(txn, isRead)
	// l.CondMutex.Lock()
	if task.Txn != nil && task.ID > l.AllowTaskID {
		l.Condition.Wait()
	}
	// l.CondMutex.Unlock()
	if task.Txn == nil {
		return fmt.Errorf("txn aborted since deadlock occured")
	}
	return nil
}

func (l *RWLock) nextTask() {
	if l.WaitingHead == nil {
		return
	}

	if l.WaitingHead.IsRead {
		maxTaskID := l.WaitingHead.ID
		l.PopTask()
		for l.WaitingHead != nil && l.WaitingHead.IsRead {
			maxTaskID = l.WaitingHead.ID
			l.PopTask()
		}

		l.AllowTaskID = maxTaskID
	} else {
		l.AllowTaskID = l.WaitingHead.ID
		l.PopTask()
	}
}

// TODO: unreenterable
// TODO: error
func (l *RWLock) RLock(txn *txns.Txn) error {
	l.Latch.Lock()
	defer l.Latch.Unlock()

	if l.WritingTxnID != 0 || l.WaitingHead != nil {
		err := l.wait(txn, true)
		if err != nil {
			return err
		}
	}

	// if there is no waiting task, then do it immediately
	l.ReadingTxnIDs[txn.ID] = struct{}{}
	l.Op.ActiveLock(l)
	return nil
}

func (l *RWLock) RUnlock(txn *txns.Txn) {
	l.Latch.Lock()
	defer l.Latch.Unlock()

	// ASSERT: reading count > 0
	delete(l.ReadingTxnIDs, txn.ID)
	if len(l.ReadingTxnIDs) == 0 {
		l.nextTask()
	}
	l.Op.InactiveLock(l)
}

// TODO: unreenterable
func (l *RWLock) Lock(txn *txns.Txn) error {
	l.Latch.Lock()
	defer l.Latch.Unlock()

	if l.WritingTxnID != 0 || l.WaitingHead != nil {
		err := l.wait(txn, false)
		if err != nil {
			return err
		}
	}

	// if there is no waiting task, then do it immediately
	atomic.StoreUint64(&l.WritingTxnID, txn.ID)
	l.Op.ActiveLock(l)
	return nil
}

func (l *RWLock) Unlock(_ *txns.Txn) {
	l.Latch.Lock()
	atomic.StoreUint64(&l.WritingTxnID, 0)
	l.Op.InactiveLock(l)
	l.nextTask()
	l.Latch.Unlock()

	l.Condition.Broadcast()
}
