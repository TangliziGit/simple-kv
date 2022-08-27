package index

import (
	"math"
	"simple-kv/txns"
)

type Version struct {
	Val string

	Deleted   bool
	StartTime uint64
	EndTime   uint64
}

func NewVersion(val string) *Version {
	return &Version{
		Val:       val,
		Deleted:   false,
		StartTime: math.MaxUint64,
		EndTime:   math.MaxUint64,
	}
}

type VersionChain struct {
	Header      *Version
	HeaderLatch string // TODO
}

func NewVersionChain() *VersionChain {
	return &VersionChain{
		Header:      nil,
		HeaderLatch: "",
	}
}

func (chain *VersionChain) Empty() bool {
	return chain.Header == nil
}

func (chain *VersionChain) Traverse(txn *txns.Txn, value *Value) *Version {
	// header.RLock
	if txn.IsWriting(value) {
		if chain.Header.Deleted {
			return nil
		} else {
			return chain.Header
		}
	}

	if txn.IsReading(value) {

	}
}
