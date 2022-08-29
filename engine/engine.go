package engine

import (
	"fmt"
	"simple-kv/gc"
	"simple-kv/index"
	lockmanager "simple-kv/locks/manager"
	"simple-kv/txns"
	txnmanager "simple-kv/txns/manager"
	"simple-kv/values"
	valuemanager "simple-kv/values/manager"
)

type Engine struct {
	Index      *index.SkipList
	Collector  *gc.GarbageCollector
	Detector   *lockmanager.DeadlockDetector
	TxnManager *txnmanager.TxnManager
}

func NewEngine() *Engine {
	valueManager := valuemanager.NewValueManager()
	lockManager := lockmanager.NewLockManager()
	txnManager := txnmanager.NewTxnManager(valueManager)
	collector := gc.NewGarbageCollector(txnManager, valueManager)
	detector := lockmanager.NewDeadlockDetector(txnManager, valueManager, lockManager)
	txnManager.SetGC(collector)

	return &Engine{
		Index:      index.NewSkipList(valueManager, lockManager),
		Collector:  collector,
		Detector:   detector,
		TxnManager: txnManager,
	}
}

func (e *Engine) Run() *Engine {
	go e.Collector.Run()
	go e.Detector.Run()
	return e
}

func (e *Engine) GetVersion(txn *txns.Txn, key uint64) (*values.Version, error) {
	val := e.Index.Get(key)
	if val == nil {
		return nil, fmt.Errorf("no such key: %d", key)
	}

	return val.Traverse(txn)
}

func (e *Engine) Get(txn *txns.Txn, key uint64) (string, error) {
	val := e.Index.Get(key)
	if val == nil {
		return "", fmt.Errorf("no such key: %d", key)
	}

	version, err := val.Traverse(txn)
	if version == nil {
		return "", fmt.Errorf("no such key: %d", key)
	}
	return version.Val, err
}

func (e *Engine) Put(txn *txns.Txn, key uint64, value string) error {
	val := e.Index.MustGet(key, value)
	writing, err := val.Put(txn, value)
	if err != nil {
		return err
	}

	if writing {
		txn.SetWriting(val.ID, key, e.Index.ID)
	}
	return nil
}

func (e *Engine) Del(txn *txns.Txn, key uint64, value string) error {
	val := e.Index.Get(key)
	if val == nil {
		return nil
	}

	writing, err := val.Del(txn)
	if writing {
		txn.SetWriting(val.ID, key, e.Index.ID)
	}
	return err
}

// Scan query `count` records sequentially from the one with key >= `key`
func (e *Engine) Scan(txn *txns.Txn, key uint64, count int) (res []string, err error) {
	for _, val := range e.Index.Scan(key, count) {
		version, err := val.Traverse(txn)
		if err != nil {
			return nil, err
		}
		res = append(res, version.Val)
	}
	return
}

func (e *Engine) NewTxn() *txns.Txn {
	return e.TxnManager.NewTxn()
}
