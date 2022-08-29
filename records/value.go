package records

type Value struct {
	VersionHeader *Version
	HeaderLatch   string // TODO
}

func NewValue(val string) *Value {
	return &Value{
		VersionHeader: NewVersion(val),
	}
}

func (v *Value) Empty() bool {
	return v.VersionHeader == nil
}

func (v *Value) Traverse(txn *Txn) *Version {
	// header.RLock
	read := func(v *Version) *Version {
		if v.Deleted {
			return nil
		} else {
			return v
		}
	}

	// TODO: move isWriting into lock
	if txn.IsWriting(v) || txn.IsReading(v) {
		return read(v.VersionHeader)
	}

	if v.Empty() {
		return nil
	}

	version := v.VersionHeader
	// TODO: check if writing
	if false {
		version = version.Next
	}

	if version.IsVisible(txn.ID) {
		if false {
			// TODO: wait writing
		}

		txn.SetReading(v)
		return read(version)
	}

	for version != nil {
		if version.IsVisible(txn.ID) {
			return read(version)
		}
		version = version.Next
	}
	return nil
}

func (v *Value) Put(txn *Txn, val string) {
	// header.wlock
	if (!txn.IsWriting(v) && false /*another one writing*/) || (!txn.IsReading(v) && false) /*another one reading*/ {
		// wait
	}

	if v.Empty() {
		v.VersionHeader = NewVersion(val)
		txn.SetWriting(v)
		return
	}

	header := v.VersionHeader
	if txn.IsWriting(v) {
		header.Val = val
		if header.Deleted {
			header.Deleted = false
		}
		return
	}

	newVersion := NewVersion(val)
	newVersion.Next = header
	header = newVersion
	txn.SetWriting(v)
}

func (v *Value) Del(txn *Txn) {
	// header.wlock
	if (!txn.IsWriting(v) && false /*another one writing*/) || (!txn.IsReading(v) && false) /*another one reading*/ {
		// wait
	}

	if v.Empty() || v.VersionHeader.Deleted {
		return
	}

	newVersion := NewVersion("")
	newVersion.Next = v.VersionHeader
	newVersion.Deleted = true
	v.VersionHeader = newVersion
	txn.SetWriting(v)
}
