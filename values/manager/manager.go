package manager

import (
	"simple-kv/locks"
	"simple-kv/values"
	"sync"
	"sync/atomic"
)

// TODO: pageable

type ValueManager struct {
	ValueCounter uint64
	ActiveValues map[uint64]*values.Value
	latch        sync.Mutex
}

func NewValueManager() *ValueManager {
	return &ValueManager{
		ValueCounter: 0,
		ActiveValues: map[uint64]*values.Value{},
		latch:        sync.Mutex{},
	}
}

func (manager *ValueManager) GetValue(valueID uint64) *values.Value {
	manager.latch.Lock()
	defer manager.latch.Unlock()
	return manager.ActiveValues[valueID]
}

func (manager *ValueManager) NewValue(lock *locks.RWLock) *values.Value {
	val := &values.Value{
		ID:            atomic.AddUint64(&manager.ValueCounter, 1),
		VersionHeader: nil,
		HeaderLock:    lock,
		Latch:         sync.Mutex{},
	}
	manager.latch.Lock()
	defer manager.latch.Unlock()
	manager.ActiveValues[val.ID] = val
	return val
}
