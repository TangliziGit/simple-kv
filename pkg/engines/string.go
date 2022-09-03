package engines

import (
	"github.com/cespare/xxhash"
	"simple-kv/pkg/txns"
	"simple-kv/pkg/values"
)

type StringEngine struct {
	uint64Engine *Uint64Engine
}

func NewStringEngine() *StringEngine {
	return &StringEngine{NewUint64Engine()}
}

func hash(key string) uint64 {
	h := xxhash.Sum64String(key)
	if h == 0 {
		h = 1
	}
	return h
}

func (e *StringEngine) Run() *StringEngine {
	e.uint64Engine.Run()
	return e
}

func (e *StringEngine) NewTxn() *txns.Txn {
	return e.uint64Engine.NewTxn()
}

func (e *StringEngine) Get(txn *txns.Txn, key string) (string, error) {
	return e.uint64Engine.Get(txn, hash(key))
}

func (e *StringEngine) Put(txn *txns.Txn, key string, value string) error {
	return e.uint64Engine.Put(txn, hash(key), value)
}

func (e *StringEngine) Del(txn *txns.Txn, key string, value string) error {
	return e.uint64Engine.Del(txn, hash(key), value)
}

func (e *StringEngine) Scan(txn *txns.Txn, key string, count int) (res []string, err error) {
	return e.uint64Engine.Scan(txn, hash(key), count)
}
func (e *StringEngine) GetVersion(txn *txns.Txn, key string) (*values.Version, error) {
	return e.uint64Engine.GetVersion(txn, hash(key))
}
