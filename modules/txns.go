package modules

import (
	"simple-kv/txns"
)

type TxnManager interface {
	GetTxn(txnID uint64) *txns.Txn
	GetActiveTxns() []*txns.Txn
	Commit(txn *txns.Txn)
	Abort(txn *txns.Txn)
}
