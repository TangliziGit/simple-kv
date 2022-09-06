package manager

import (
	"simple-kv/pkg/locks"
	"simple-kv/pkg/values"
	"sync"
	"sync/atomic"
)

// TODO: use pages to store values?

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

func (manager *ValueManager) DelValue(valueID uint64) {
	manager.latch.Lock()
	defer manager.latch.Unlock()
	delete(manager.ActiveValues, valueID)
}
