package modules

import (
	"simple-kv/locks"
	"simple-kv/values"
)

type ValueManager interface {
	NewValue(lock *locks.RWLock) *values.Value
	GetValue(txnID uint64) *values.Value
}
