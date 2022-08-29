package records

import (
	"sync"
	"sync/atomic"
)

var txnCounter = uint64(0)

type Txn struct {
	ID uint64
	// ReadSet is to release read lock
	ReadSet map[*Value]struct{}
	// WriteSet is to abort, GC or release write lock
	WriteSet map[*Value]struct{}
	Latch    sync.Mutex
}

// TODO: rwlock
func NewTxn() *Txn {
	// TODO: atomic id
	return &Txn{
		ID:       atomic.AddUint64(&txnCounter, 1),
		ReadSet:  map[*Value]struct{}{},
		WriteSet: map[*Value]struct{}{},
		Latch:    sync.Mutex{},
	}
}

func (txn *Txn) IsWriting(value *Value) bool {
	_, exist := txn.WriteSet[value]
	return exist
}

func (txn *Txn) IsReading(value *Value) bool {
	_, exist := txn.ReadSet[value]
	return exist
}

func (txn *Txn) SetWriting(value *Value) {
	txn.WriteSet[value] = struct{}{}
}

func (txn *Txn) SetReading(value *Value) {
	txn.ReadSet[value] = struct{}{}
}
