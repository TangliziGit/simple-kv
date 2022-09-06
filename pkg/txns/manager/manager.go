package manager

import (
	"fmt"
	modules2 "simple-kv/pkg/modules"
	"simple-kv/pkg/txns"
	"sync"
	"sync/atomic"
)

type TxnManager struct {
	TxnCounter   uint64
	ActiveTxns   map[uint64]*txns.Txn
	ValueManager modules2.ValueManager
	GC           modules2.GarbageCollector
	latch        sync.Mutex
}

func NewTxnManager(valueManager modules2.ValueManager) *TxnManager {
	return &TxnManager{
		TxnCounter:   0,
		ActiveTxns:   map[uint64]*txns.Txn{},
		ValueManager: valueManager,
		GC:           nil,
		latch:        sync.Mutex{},
	}
}

func (manager *TxnManager) SetGC(gc modules2.GarbageCollector) {
	manager.GC = gc
}

func (manager *TxnManager) NewTxn() *txns.Txn {
	txn := &txns.Txn{
		ID:       atomic.AddUint64(&manager.TxnCounter, 1),
		State:    txns.Processing,
		Waiting:  false,
		ReadSet:  map[uint64]struct{}{},
		WriteSet: map[uint64]*txns.WriteInfo{},
		Latch:    sync.Mutex{},
		Op:       manager,
	}
	manager.latch.Lock()
	manager.ActiveTxns[txn.ID] = txn
	manager.latch.Unlock()
	return txn
}

func (manager *TxnManager) GetTxn(txnID uint64) *txns.Txn {
	manager.latch.Lock()
	defer manager.latch.Unlock()
	return manager.ActiveTxns[txnID]
}

func (manager *TxnManager) GetActiveTxns() (res []*txns.Txn) {
	manager.latch.Lock()
	defer manager.latch.Unlock()
	for _, txn := range manager.ActiveTxns {
		res = append(res, txn)
	}
	return
}

func (manager *TxnManager) Commit(txn *txns.Txn) error {
	if txn.State != txns.Processing {
		return fmt.Errorf("fail to commit: state=%v", txn.State)
	}

	txn.CommitID = atomic.AddUint64(&manager.TxnCounter, 1)

	for valID := range txn.ReadSet {
		val := manager.ValueManager.GetValue(valID)
		val.HeaderLock.RUnlock(txn)
	}

	for valID := range txn.WriteSet {
		val := manager.ValueManager.GetValue(valID)
		val.VersionHeader.Install(txn.CommitID)
		val.HeaderLock.Unlock(txn)
	}

	txn.State = txns.Committed
	manager.latch.Lock()
	delete(manager.ActiveTxns, txn.ID)
	manager.latch.Unlock()
	manager.GC.Register(txn)
	return nil
}

func (manager *TxnManager) Abort(txn *txns.Txn) error {
	if txn.State != txns.Processing {
		return fmt.Errorf("fail to commit: state=%v", txn.State)
	}
	manager.latch.Lock()
	delete(manager.ActiveTxns, txn.ID)
	manager.latch.Unlock()

	for valID := range txn.ReadSet {
		val := manager.ValueManager.GetValue(valID)
		val.HeaderLock.RUnlock(txn)
	}

	for valID := range txn.WriteSet {
		val := manager.ValueManager.GetValue(valID)
		val.VersionHeader = val.VersionHeader.Next
		val.HeaderLock.Unlock(txn)
	}

	txn.State = txns.Aborted
	return nil
}
