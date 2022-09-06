package txns

import "sync"

type State int

const (
	Processing State = iota
	Committed
	Aborted
)

type WriteInfo struct {
	Key     uint64
	IndexID uint64
}

func NewWriteInfo(key uint64, index uint64) *WriteInfo {
	return &WriteInfo{
		Key:     key,
		IndexID: index,
	}
}

type Operator interface {
	Commit(txn *Txn) error
	Abort(txn *Txn) error
}

type Txn struct {
	ID       uint64
	State    State
	Waiting  bool
	CommitID uint64
	// ReadSet is to release read lock
	ReadSet map[uint64]struct{}
	// WriteSet is to Abort, GC or release write lock
	WriteSet map[uint64]*WriteInfo
	Latch    sync.Mutex
	Op       Operator
}

func (txn *Txn) IsWriting(valueID uint64) bool {
	_, exist := txn.WriteSet[valueID]
	return exist
}

func (txn *Txn) IsReading(valueID uint64) bool {
	_, exist := txn.ReadSet[valueID]
	return exist
}

func (txn *Txn) SetWriting(valueID uint64, key uint64, indexID uint64) {
	txn.WriteSet[valueID] = NewWriteInfo(key, indexID)
}

func (txn *Txn) SetReading(valueID uint64) {
	txn.ReadSet[valueID] = struct{}{}
}

func (txn *Txn) Commit() error {
	return txn.Op.Commit(txn)
}

func (txn *Txn) Abort() error {
	return txn.Op.Abort(txn)
}
