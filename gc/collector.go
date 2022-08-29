package gc

import (
	"math"
	"simple-kv/index"
	txns2 "simple-kv/modules"
	"simple-kv/txns"
	"simple-kv/values"
	"time"
)

type GarbageCollector struct {
	cleanChan     chan *txns.Txn
	NeedCleanTxns []*txns.Txn
	TxnManager    txns2.TxnManager
	ValueManager  txns2.ValueManager
}

func NewGarbageCollector(txnManager txns2.TxnManager, valueManager txns2.ValueManager) *GarbageCollector {
	return &GarbageCollector{
		// infinity chan?
		cleanChan:     make(chan *txns.Txn, 1000),
		NeedCleanTxns: []*txns.Txn{},
		TxnManager:    txnManager,
		ValueManager:  valueManager,
	}
}

func (g *GarbageCollector) Run() {
	for range time.Tick(time.Millisecond * 50) {
		g.Clean()
	}
}

func (g *GarbageCollector) Register(txn *txns.Txn) {
	g.cleanChan <- txn
}

func (g *GarbageCollector) Clean() {
	minTxnID := uint64(math.MaxUint64)
	for _, txn := range g.TxnManager.GetActiveTxns() {
		if txn.ID < minTxnID {
			minTxnID = txn.ID
		}
	}

Loop:
	for {
		select {
		case txn := <-g.cleanChan:
			g.NeedCleanTxns = append(g.NeedCleanTxns, txn)
		default:
			break Loop
		}
	}

	var nextTurnTxns []*txns.Txn
	for _, txn := range g.NeedCleanTxns {
		if txn.CommitID < minTxnID {
			if cleanAgain := g.Collect(txn); cleanAgain {
				nextTurnTxns = append(nextTurnTxns, txn)
			}
		} else {
			nextTurnTxns = append(nextTurnTxns, txn)
		}
	}
	g.NeedCleanTxns = nextTurnTxns
}

func (g *GarbageCollector) Collect(txn *txns.Txn) bool {
	cleanAgain := false
	newWriteSet := map[uint64]*txns.WriteInfo{}
	for valID, info := range txn.WriteSet {
		val := g.ValueManager.GetValue(valID)
		cleanAgain = cleanAgain || g.Truncate(val, txn.CommitID)

		if !cleanAgain {
			newWriteSet[valID] = info
		}

		if val.VersionHeader == nil {
			index.ActiveIndex[info.IndexID].Vacuum(info.Key)
			// TODO: del active val
		}
	}
	txn.WriteSet = newWriteSet

	return cleanAgain
}

func (g *GarbageCollector) Truncate(val *values.Value, commitID uint64) bool {
	prev := values.NewVersion("")
	prev.Next = val.VersionHeader
	iter := prev.Next
	for iter != nil && !iter.IsVisible(commitID) {
		prev = iter
		iter = iter.Next
	}

	if iter.Next == nil {
		return false
	}
	iter.Next = nil

	cleanAgain := false
	if iter.Deleted {
		if iter == val.VersionHeader {
			if val.HeaderLock.WritingTxnID == 0 && len(val.HeaderLock.ReadingTxnIDs) == 0 {
				val.VersionHeader = nil
			} else {
				cleanAgain = true
			}
		} else {
			prev.Next = nil
		}
	}
	return cleanAgain
}
