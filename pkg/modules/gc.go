package modules

import (
	"simple-kv/pkg/txns"
)

type GarbageCollector interface {
	Register(txn *txns.Txn)
}
