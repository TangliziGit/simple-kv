package modules

import "simple-kv/locks"

type LockManager interface {
	NewRWLock() *locks.RWLock
}
