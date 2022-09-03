package index

import (
	"math/rand"
	"simple-kv/pkg/config"
	modules2 "simple-kv/pkg/modules"
	"simple-kv/pkg/values"
	"sync"
	"sync/atomic"
	"time"
)

var (
	indexCounter = uint64(0)
	ActiveIndex  = map[uint64]*SkipList{}
)

type SkipNode struct {
	Key uint64
	Val *values.Value

	Nexts []*SkipNode
	Level int
}

func (s *SkipList) NewSkipNode(key uint64, val string) *SkipNode {
	nexts := make([]*SkipNode, config.SkipListMaxLevel)
	for i := range nexts {
		nexts[i] = nil
	}

	return &SkipNode{
		Key:   key,
		Val:   s.ValueManager.NewValue(s.LockManager.NewRWLock()),
		Nexts: nexts,
		Level: 0,
	}
}

type SkipList struct {
	ID           uint64
	Header       *SkipNode
	Level        int
	ValueManager modules2.ValueManager
	LockManager  modules2.LockManager
	latch        sync.Mutex
}

func NewSkipList(valueManager modules2.ValueManager, lockManager modules2.LockManager) *SkipList {
	rand.Seed(time.Now().Unix())
	index := &SkipList{
		ID:           atomic.AddUint64(&indexCounter, 1),
		Level:        0,
		ValueManager: valueManager,
		LockManager:  lockManager,
		latch:        sync.Mutex{},
	}
	index.Header = index.NewSkipNode(0, "HEADER")
	ActiveIndex[index.ID] = index
	return index
}

func (s *SkipList) Get(key uint64) *values.Value {
	if key == 0 {
		return nil
	}

	s.latch.Lock()
	defer s.latch.Unlock()

	node := s.Header
	for i := s.Level; i >= 0; i-- {
		for node.Nexts[i] != nil && node.Nexts[i].Key < key {
			node = node.Nexts[i]
		}
	}

	node = node.Nexts[0]
	if node == nil || node.Key != key {
		return nil
	}
	return node.Val
}

// TODO: deal with 0!
func (s *SkipList) MustGet(key uint64, val string) *values.Value {
	if key == 0 {
		return nil
	}

	s.latch.Lock()
	defer s.latch.Unlock()

	node := s.Header
	updates := make([]*SkipNode, config.SkipListMaxLevel)
	for i := s.Level; i >= 0; i-- {
		for node.Nexts[i] != nil && node.Nexts[i].Key < key {
			node = node.Nexts[i]
		}
		updates[i] = node
	}

	node = node.Nexts[0]
	if node != nil && node.Key == key {
		return node.Val
	}

	newNode := s.NewSkipNode(key, val)
	newNode.Level = getLevel()
	if newNode.Level > s.Level {
		for i := s.Level + 1; i <= newNode.Level; i++ {
			updates[i] = s.Header
		}
		s.Level = newNode.Level
		s.Header.Level = newNode.Level
	}

	for i := 0; i <= newNode.Level; i++ {
		newNode.Nexts[i] = updates[i].Nexts[i]
		updates[i].Nexts[i] = newNode
	}
	return newNode.Val
}

// TODO: need test
// Scan query `count` records sequentially from the one with key >= `key`
func (s *SkipList) Scan(key uint64, count int) []*values.Value {
	if key == 0 {
		return nil
	}

	s.latch.Lock()
	defer s.latch.Unlock()

	node := s.Header
	for i := s.Level; i >= 0; i-- {
		for node.Nexts[i] != nil && node.Nexts[i].Key < key {
			node = node.Nexts[i]
		}
	}

	node = node.Nexts[0]
	var result []*values.Value
	for node != nil && count > 0 {
		result = append(result, node.Val)
		node = node.Nexts[0]
		count--
	}
	return result
}

func (s *SkipList) Vacuum(key uint64) bool {
	s.latch.Lock()
	defer s.latch.Unlock()

	if key == 0 {
		return false
	}

	updates := make([]*SkipNode, s.Level+1)

	node := s.Header
	for i := s.Level; i >= 0; i-- {
		for node.Nexts[i] != nil && node.Nexts[i].Key < key {
			node = node.Nexts[i]
		}
		updates[i] = node
	}

	node = node.Nexts[0]
	for i := 0; i <= s.Level; i++ {
		next := updates[i].Nexts[i]
		// TODO: if next == nil ?
		if next != nil && next.Key != key {
			break
		}
		updates[i].Nexts[i] = next.Nexts[i]
	}

	l := node.Level
	for l > 0 && s.Header.Nexts[l] == nil {
		s.Level--
		l--
	}
	return true
}

func getLevel() int {
	level := 0
	for rand.Float64() < config.SkipListProp && level < config.SkipListMaxLevel {
		level++
	}
	return level
}
