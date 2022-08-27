package index

import (
	"strconv"
	"testing"
)

func TestSkipList_Put_Basic(t *testing.T) {
	s := NewSkipList()
	s.Put(30, "30")
	s.Put(50, "50")
	s.Put(40, "40")
	s.Put(20, "20")

	node := s.Get(20)
	if node == nil {
		t.Errorf("Expect 20, got nil\n")
	} else if node.Val != "20" {
		t.Errorf("Expect 20, got %s\n", node.Val)
	}
}

func TestSkipList_Put_Large(t *testing.T) {
	s := NewSkipList()
	for i := 1; i < 100000; i++ {
		s.Put(uint64(i), strconv.Itoa(i))
	}

	for i := 1; i < 100000; i++ {
		node := s.Get(uint64(i))
		if node == nil {
			t.Errorf("Expect %d, got nil\n", i)
		} else if node.Val != strconv.Itoa(i) {
			t.Errorf("Expect %d, got %s\n", i, node.Val)
		}
	}

	for i := 1; i < 100000; i++ {
		s.Put(uint64(i), strconv.Itoa(i+1))
	}

	for i := 1; i < 100000; i++ {
		node := s.Get(uint64(i))
		if node == nil {
			t.Errorf("Expect %d, got nil\n", i+1)
		} else if node.Val != strconv.Itoa(i+1) {
			t.Errorf("Expect %d, got %s\n", i+1, node.Val)
		}
	}
}
func TestSkipList_Del_Basic(t *testing.T) {
	s := NewSkipList()
	s.Put(30, "30")
	s.Put(50, "50")
	s.Put(40, "40")
	s.Put(20, "20")

	s.Del(20)

	node := s.Get(20)
	if node != nil {
		t.Errorf("Expect nil, got %s\n", node.Val)
	}
}

func TestSkipList_Del_Large(t *testing.T) {
	s := NewSkipList()
	for i := 1; i < 100000; i++ {
		s.Put(uint64(i), strconv.Itoa(i))
	}

	for i := 1; i < 100000; i++ {
		s.Del(uint64(i))
	}

	for i := 1; i < 100000; i++ {
		node := s.Get(uint64(i))

		if node != nil {
			t.Errorf("Expect nil, got %s\n", node.Val)
		}
	}

	if s.Level != 0 {
		t.Errorf("Expect level equals 0, got %d\n", s.Level)
	}
}
