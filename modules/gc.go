package modules

import "simple-kv/txns"

type GarbageCollector interface {
	Register(txn *txns.Txn)
}
