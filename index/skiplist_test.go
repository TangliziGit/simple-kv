package index

import (
	"simple-kv/records"
	"strconv"
	"testing"
)

func TestSkipList_Put_Basic(t *testing.T) {
	txn := records.NewTxn()
	s := NewSkipList()
	s.Put(txn, 30, "30")
	s.Put(txn, 50, "50")
	s.Put(txn, 40, "40")
	s.Put(txn, 20, "20")

	node := s.Get(txn, 20)
	if node == nil {
		t.Errorf("Expect 20, got nil\n")
	} else if node.Val != "20" {
		t.Errorf("Expect 20, got %s\n", node.Val)
	}

	if len(txn.WriteSet) != 4 {
		t.Errorf("Expect 4, got %v\n", txn.WriteSet)
	}
	if len(txn.ReadSet) != 0 {
		t.Errorf("Expect 0, got %v\n", txn.ReadSet)
	}
}

func TestSkipList_Put_Large(t *testing.T) {
	const scale = 100000

	txn := records.NewTxn()
	s := NewSkipList()
	for i := 1; i < scale; i++ {
		s.Put(txn, uint64(i), strconv.Itoa(i))
	}

	for i := 1; i < scale; i++ {
		node := s.Get(txn, uint64(i))
		if node == nil {
			t.Errorf("Expect %d, got nil\n", i)
		} else if node.Val != strconv.Itoa(i) {
			t.Errorf("Expect %d, got %s\n", i, node.Val)
		}
	}

	for i := 1; i < scale; i++ {
		s.Put(txn, uint64(i), strconv.Itoa(i+1))
	}

	for i := 1; i < scale; i++ {
		node := s.Get(txn, uint64(i))
		if node == nil {
			t.Errorf("Expect %d, got nil\n", i+1)
		} else if node.Val != strconv.Itoa(i+1) {
			t.Errorf("Expect %d, got %s\n", i+1, node.Val)
		}
	}
}

func TestSkipList_Del_Basic(t *testing.T) {
	txn := records.NewTxn()
	s := NewSkipList()
	s.Put(txn, 30, "30")
	s.Put(txn, 50, "50")
	s.Put(txn, 40, "40")
	s.Put(txn, 20, "20")

	s.Del(txn, 20)

	node := s.Get(txn, 20)
	if node != nil {
		t.Errorf("Expect nil, got %s\n", node.Val)
	}
}

func TestSkipList_Del_Large(t *testing.T) {
	const scale = 100000

	txn := records.NewTxn()
	s := NewSkipList()
	for i := 1; i < scale; i++ {
		s.Put(txn, uint64(i), strconv.Itoa(i))
	}

	for i := 1; i < scale; i++ {
		s.Del(txn, uint64(i))
	}

	for i := 1; i < scale; i++ {
		node := s.Get(txn, uint64(i))
		if node != nil {
			t.Errorf("Expect nil, got %s\n", node.Val)
		}
	}
}
