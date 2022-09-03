package index

import (
	"simple-kv/pkg/locks/manager"
	manager2 "simple-kv/pkg/values/manager"
	"strconv"
	"testing"
)

func TestSkipList_Put_Basic(t *testing.T) {
	lockMgr := manager.NewLockManager()
	valMgr := manager2.NewValueManager()
	s := NewSkipList(valMgr, lockMgr)
	s.MustGet(30, "30")
	s.MustGet(50, "50")
	s.MustGet(40, "40")
	s.MustGet(20, "20")

	value := s.Get(20)
	if value == nil {
		t.Errorf("Expect non-nil, got nil\n")
	} else if value.VersionHeader != nil {
		t.Errorf("Expect nil, got %v\n", value.VersionHeader)
	}
}

func TestSkipList_Put_Large(t *testing.T) {
	const scale = 100000

	lockMgr := manager.NewLockManager()
	valMgr := manager2.NewValueManager()
	s := NewSkipList(valMgr, lockMgr)
	for i := 1; i < scale; i++ {
		s.MustGet(uint64(i), strconv.Itoa(i))
	}

	for i := 1; i < scale; i++ {
		value := s.Get(uint64(i))
		if value == nil {
			t.Errorf("Expect non-nil, got nil\n")
		} else if value.VersionHeader != nil {
			t.Errorf("Expect nil, got %v\n", value.VersionHeader)
		}
	}

	for i := 1; i < scale; i++ {
		s.MustGet(uint64(i), strconv.Itoa(i+1))
	}

	for i := 1; i < scale; i++ {
		value := s.Get(uint64(i))
		if value == nil {
			t.Errorf("Expect non-nil, got nil\n")
		} else if value.VersionHeader != nil {
			t.Errorf("Expect nil, got %v\n", value.VersionHeader)
		}
	}
}

func TestSkipList_Del_Basic(t *testing.T) {
	lockMgr := manager.NewLockManager()
	valMgr := manager2.NewValueManager()
	s := NewSkipList(valMgr, lockMgr)
	s.MustGet(30, "30")
	s.MustGet(50, "50")
	s.MustGet(40, "40")
	s.MustGet(20, "20")

	s.Vacuum(20)

	version := s.Get(20)
	if version != nil {
		t.Errorf("Expect nil, got %v\n", version)
	}
}

func TestSkipList_Del_Large(t *testing.T) {
	const scale = 100000

	lockMgr := manager.NewLockManager()
	valMgr := manager2.NewValueManager()
	s := NewSkipList(valMgr, lockMgr)
	for i := 1; i < scale; i++ {
		s.MustGet(uint64(i), strconv.Itoa(i))
	}

	for i := 1; i < scale; i++ {
		s.Vacuum(uint64(i))
	}

	for i := 1; i < scale; i++ {
		version := s.Get(uint64(i))
		if version != nil {
			t.Errorf("Expect nil, got %v\n", version)
		}
	}
}
