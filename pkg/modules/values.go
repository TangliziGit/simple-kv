package modules

import (
	"simple-kv/pkg/locks"
	"simple-kv/pkg/values"
)

type ValueManager interface {
	NewValue(lock *locks.RWLock) *values.Value
	GetValue(valueID uint64) *values.Value
	DelValue(valueID uint64)
}
