package values

import (
	"simple-kv/pkg/locks"
	"simple-kv/pkg/txns"
	"sync"
)

type Value struct {
	ID            uint64
	VersionHeader *Version
	HeaderLock    *locks.RWLock
	Latch         sync.Mutex
}

func (v *Value) Traverse(txn *txns.Txn) (*Version, error) {
	v.Latch.Lock()
	defer v.Latch.Unlock()

	read := func(v *Version) *Version {
		if v.Deleted {
			return nil
		} else {
			return v
		}
	}

	lock := v.HeaderLock
	if lock.WritingTxnID == txn.ID {
		return read(v.VersionHeader), nil
	} else if _, ok := lock.ReadingTxnIDs[txn.ID]; ok {
		return read(v.VersionHeader), nil
	}

	if v.VersionHeader == nil {
		return nil, nil
	}

	version := v.VersionHeader
	if v.HeaderLock.WritingTxnID != 0 {
		version = version.Next
	}

	if version == nil {
		return nil, nil
	}

	if version.IsVisible(txn.ID) {
		err := v.HeaderLock.RLock(txn)
		if err != nil {
			return nil, err
		}

		txn.SetReading(v.ID)
		return read(version), nil
	}

	for version != nil {
		if version.IsVisible(txn.ID) {
			return read(version), nil
		}
		version = version.Next
	}
	return nil, nil
}

func (v *Value) Put(txn *txns.Txn, val string) (bool, error) {
	v.Latch.Lock()
	defer v.Latch.Unlock()

	if v.HeaderLock.WritingTxnID != txn.ID {
		err := v.HeaderLock.Lock(txn)
		if err != nil {
			return false, err
		}
	}

	if v.VersionHeader == nil {
		v.VersionHeader = NewVersion(val)
		return true, nil
	}

	header := v.VersionHeader
	if txn.IsWriting(v.ID) {
		header.Val = val
		if header.Deleted {
			header.Deleted = false
		}
		return false, nil
	}

	newVersion := NewVersion(val)
	newVersion.Next = header
	v.VersionHeader = newVersion
	return true, nil
}

func (v *Value) Del(txn *txns.Txn) (bool, error) {
	v.Latch.Lock()
	defer v.Latch.Unlock()

	if v.HeaderLock.WritingTxnID != txn.ID {
		err := v.HeaderLock.Lock(txn)
		if err != nil {
			return false, err
		}
	}

	if v.VersionHeader == nil || v.VersionHeader.Deleted {
		return false, nil
	}

	newVersion := NewVersion("")
	newVersion.Next = v.VersionHeader
	newVersion.Deleted = true
	v.VersionHeader = newVersion
	return true, nil
}
