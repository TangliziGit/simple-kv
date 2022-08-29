package manager

import (
	"simple-kv/locks"
	"sync"
)

type LockManager struct {
	ActiveLocks map[*locks.RWLock]struct{}
	latch       sync.Mutex
}

func NewLockManager() *LockManager {
	return &LockManager{
		ActiveLocks: map[*locks.RWLock]struct{}{},
		latch:       sync.Mutex{},
	}
}

func (manager *LockManager) GetActiveLocks() (res []*locks.RWLock) {
	manager.latch.Lock()
	defer manager.latch.Unlock()
	for lock := range manager.ActiveLocks {
		res = append(res, lock)
	}
	return
}

func (manager *LockManager) ActiveLock(lock *locks.RWLock) {
	manager.latch.Lock()
	defer manager.latch.Unlock()

	manager.ActiveLocks[lock] = struct{}{}
}

func (manager *LockManager) InactiveLock(lock *locks.RWLock) {
	manager.latch.Lock()
	defer manager.latch.Unlock()

	delete(manager.ActiveLocks, lock)
}

func (manager *LockManager) NewRWLock() *locks.RWLock {
	mutex := sync.Mutex{}
	return &locks.RWLock{
		WaitingHead:   nil,
		WaitingTail:   nil,
		Condition:     sync.NewCond(&mutex),
		WritingTxnID:  0,
		ReadingTxnIDs: map[uint64]struct{}{},
		Op:            manager,
		Latch:         &mutex,
	}
}
