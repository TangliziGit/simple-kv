package modules

import (
	"simple-kv/pkg/locks"
)

type LockManager interface {
	NewRWLock() *locks.RWLock
}
