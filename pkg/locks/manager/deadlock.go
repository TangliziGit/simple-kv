package manager

import (
	"simple-kv/pkg/locks"
	modules2 "simple-kv/pkg/modules"
	"simple-kv/pkg/txns"
	"sync"
	"time"
)

type Node struct {
	isTxn bool
	Txn   *txns.Txn
	Lock  *locks.RWLock

	InDegree int
	Nexts    map[*Node]struct{}
	Prevs    map[*Node]struct{}
}

func NewNode(isTxn bool, txn *txns.Txn, lock *locks.RWLock) *Node {
	return &Node{
		isTxn:    isTxn,
		Txn:      txn,
		Lock:     lock,
		InDegree: 0,
		Nexts:    map[*Node]struct{}{},
		Prevs:    map[*Node]struct{}{},
	}
}

type DeadlockDetector struct {
	TxnManager   modules2.TxnManager
	ValueManager modules2.ValueManager
	LockManager  *LockManager
	latch        sync.Mutex
}

func NewDeadlockDetector(txnManager modules2.TxnManager, valueManager modules2.ValueManager,
	lockManager *LockManager) *DeadlockDetector {
	return &DeadlockDetector{
		TxnManager:   txnManager,
		ValueManager: valueManager,
		LockManager:  lockManager,
		latch:        sync.Mutex{},
	}
}

func (d *DeadlockDetector) Run() {
	for _ = range time.Tick(time.Millisecond * 50) {
		d.Detect()
	}
}

func (d *DeadlockDetector) Detect() {
	lockNodes := map[*locks.RWLock]*Node{}
	txnNodes := map[uint64]*Node{}
	for _, lock := range d.LockManager.GetActiveLocks() {
		lockNode := NewNode(false, nil, lock)
		lockNodes[lock] = lockNode

		if lock.GetWritingTxnID() != 0 {
			txnID := lock.GetWritingTxnID()
			if _, exists := txnNodes[txnID]; !exists {
				txnNodes[txnID] = NewNode(true, d.TxnManager.GetTxn(txnID), nil)
			}
			txnNodes[txnID].Nexts[lockNode] = struct{}{}
			lockNode.Prevs[txnNodes[txnID]] = struct{}{}
			lockNode.InDegree++
		}

		for txnID := range lock.GetReadingTxnIDs() {
			if _, exists := txnNodes[txnID]; !exists {
				txnNodes[txnID] = NewNode(true, d.TxnManager.GetTxn(txnID), nil)
			}
			txnNodes[txnID].Nexts[lockNode] = struct{}{}
			lockNode.Prevs[txnNodes[txnID]] = struct{}{}
			lockNode.InDegree++
		}

		waitingTxn := lock.WaitingHead
		for waitingTxn != nil {
			txnID := waitingTxn.Txn.ID
			if _, exists := txnNodes[txnID]; !exists {
				txnNodes[txnID] = NewNode(true, waitingTxn.Txn, nil)
			}
			lockNode.Nexts[txnNodes[txnID]] = struct{}{}
			txnNodes[txnID].Prevs[lockNode] = struct{}{}
			txnNodes[txnID].InDegree++

			waitingTxn = waitingTxn.Next
		}
	}

	nodes := map[*Node]struct{}{}
	var que []*Node
	for _, node := range lockNodes {
		nodes[node] = struct{}{}
		if node.InDegree == 0 {
			que = append(que, node)
		}
	}
	for _, node := range txnNodes {
		nodes[node] = struct{}{}
		if node.InDegree == 0 {
			que = append(que, node)
		}
	}

	for len(nodes) != 0 {
		for len(que) != 0 {
			node := que[0]
			que = que[1:]

			for next := range node.Nexts {
				next.InDegree--
				if next.InDegree == 0 {
					que = append(que, next)
				}
			}
			delete(nodes, node)
		}

		for node := range nodes {
			if node.isTxn == false {
				continue
			}

			// rollback txn and release locks
			txn := node.Txn
			for valID := range txn.WriteSet {
				val := d.ValueManager.GetValue(valID)
				lockNode := lockNodes[val.HeaderLock]
				txnNode := txnNodes[txn.ID]

				delete(txnNode.Nexts, lockNode)
				delete(lockNode.Prevs, txnNode)
				lockNode.InDegree--
				if lockNode.InDegree == 0 {
					que = append(que, lockNode)
				}
			}
			for valID := range txn.ReadSet {
				val := d.ValueManager.GetValue(valID)
				lockNode := lockNodes[val.HeaderLock]
				txnNode := txnNodes[txn.ID]

				delete(txnNode.Nexts, lockNode)
				delete(lockNode.Prevs, txnNode)
				lockNode.InDegree--
				if lockNode.InDegree == 0 {
					que = append(que, lockNode)
				}
			}

			for waitingLockNode := range node.Prevs {
				lock := waitingLockNode.Lock
				lock.CancelTask(node.Txn)
				delete(waitingLockNode.Nexts, node)
			}
			que = append(que, node)

			node.Txn.Abort()
			break
		}
	}
}
