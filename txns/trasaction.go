package txns

import (
	"simple-kv/index"
	"sync"
)

type Txn struct {
	ID       uint64
	ReadSet  map[*index.Value]struct{}
	WriteSet map[*index.Value]struct{}
	Latch    sync.Mutex
}

func (txn *Txn) IsWriting(value *index.Value) bool {
	_, exist := txn.WriteSet[value]
	return exist
}

func (txn *Txn) IsReading(value *index.Value) bool {
	_, exist := txn.ReadSet[value]
	return exist
}
