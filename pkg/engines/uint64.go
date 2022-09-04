package engines

import (
	"fmt"
	"simple-kv/pkg/gc"
	"simple-kv/pkg/index"
	"simple-kv/pkg/locks/manager"
	"simple-kv/pkg/txns"
	txnmanager "simple-kv/pkg/txns/manager"
	"simple-kv/pkg/values"
	valuemanager "simple-kv/pkg/values/manager"
)

type Uint64Engine struct {
	Index      *index.SkipList
	Collector  *gc.GarbageCollector
	Detector   *manager.DeadlockDetector
	TxnManager *txnmanager.TxnManager
}

func NewUint64Engine() *Uint64Engine {
	valueManager := valuemanager.NewValueManager()
	lockManager := manager.NewLockManager()
	txnManager := txnmanager.NewTxnManager(valueManager)
	collector := gc.NewGarbageCollector(txnManager, valueManager)
	detector := manager.NewDeadlockDetector(txnManager, valueManager, lockManager)
	txnManager.SetGC(collector)

	return &Uint64Engine{
		Index:      index.NewSkipList(valueManager, lockManager),
		Collector:  collector,
		Detector:   detector,
		TxnManager: txnManager,
	}
}

func (e *Uint64Engine) Run() *Uint64Engine {
	go e.Collector.Run()
	go e.Detector.Run()
	return e
}

func (e *Uint64Engine) GetVersion(txn *txns.Txn, key uint64) (*values.Version, error) {
	val := e.Index.Get(key)
	if val == nil {
		return nil, fmt.Errorf("no such key: %d", key)
	}

	return val.Traverse(txn)
}

func (e *Uint64Engine) Get(txn *txns.Txn, key uint64) (string, error) {
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

func (e *Uint64Engine) Put(txn *txns.Txn, key uint64, value string) error {
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

func (e *Uint64Engine) Del(txn *txns.Txn, key uint64) error {
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
func (e *Uint64Engine) Scan(txn *txns.Txn, key uint64, count int) (res []string, err error) {
	for _, val := range e.Index.Scan(key, count) {
		version, err := val.Traverse(txn)
		if err != nil {
			return nil, err
		}
		res = append(res, version.Val)
	}
	return
}

func (e *Uint64Engine) NewTxn() *txns.Txn {
	return e.TxnManager.NewTxn()
}
