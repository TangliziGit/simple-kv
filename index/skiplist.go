package index

import (
	"math/rand"
	"simple-kv/config"
	"simple-kv/txns"
	"time"
)

type SkipNode struct {
	Key uint64
	Val *Value

	Nexts []*SkipNode
	Level int
}

func NewSkipNode(key uint64, val string) *SkipNode {
	nexts := make([]*SkipNode, config.SkipListMaxLevel)
	for i := range nexts {
		nexts[i] = nil
	}

	return &SkipNode{
		Key:   key,
		Val:   NewValue(val),
		Nexts: nexts,
		Level: 0,
	}
}

type SkipList struct {
	Header *SkipNode
	Level  int
}

func NewSkipList() *SkipList {
	rand.Seed(time.Now().Unix())
	return &SkipList{
		Header: NewSkipNode(0, "HEADER"),
		Level:  0,
	}
}

func (s *SkipList) Get(txn *txns.Txn, key uint64) *Version {
	if key == 0 {
		return nil
	}

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
	return node
}

func (s *SkipList) Put(txn *txns.Txn, key uint64, val string) {
	if key == 0 {
		return
	}

	updates := make([]*SkipNode, config.SkipListMaxLevel)

	node := s.Header
	for i := s.Level; i >= 0; i-- {
		for node.Nexts[i] != nil && node.Nexts[i].Key < key {
			node = node.Nexts[i]
		}
		updates[i] = node
	}

	node = node.Nexts[0]
	if node != nil && node.Key == key {
		node.Val = val
		return
	}

	newNode := NewSkipNode(key, val)
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
}

func (s *SkipList) Del(txn *txns.Txn, key uint64) {
	if key == 0 {
		return
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
	if node == nil || node.Key != key {
		return
	}

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
}

func (s *SkipList) Scan() {
	// TODO
}

func getLevel() int {
	level := 0
	for rand.Float64() < config.SkipListProp && level < config.SkipListMaxLevel {
		level++
	}
	return level
}
