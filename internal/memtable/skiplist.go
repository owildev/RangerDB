package memtable

import (
	"bytes"
	"math/rand/v2"
	"sync"
)

const (
	maxLevels = 16
	topLevel  = maxLevels - 1
)

type node struct {
	key       []byte
	value     []byte
	tombstone bool
	next      [maxLevels]*node
}

type SkipList struct {
	rwMu sync.RWMutex
	head [maxLevels]*node
}

func getHeight() int {
	h := 1
	for h < maxLevels && rand.IntN(4) == 0 {
		h++
	}
	return h - 1
}

func NewSkipList() *SkipList {
	return &SkipList{}
}

func (list *SkipList) Put(key, value []byte) {
	list.put(key, value, false)
}

func (list *SkipList) PutTombstone(key []byte) {
	list.put(key, nil, true)
}

func (list *SkipList) put(key, value []byte, tomb bool) {
	height := getHeight()
	newNode := &node{key: key, value: value, tombstone: tomb}

	list.rwMu.Lock()
	defer list.rwMu.Unlock()

	curr := list.head[topLevel]
	var prev *node
	for level := topLevel; level >= 0; {
		for curr != nil && bytes.Compare(curr.key, key) < 0 {
			prev = curr
			curr = curr.next[level]
		}

		// Overwrite or delete case
		if curr != nil && bytes.Equal(key, curr.key) {
			curr.value = bytes.Clone(value)
			curr.tombstone = tomb
			return
		}

		// Insert case
		if prev == nil {
			if level <= height {
				list.head[level] = newNode
				newNode.next[level] = curr
			}
			level--
			if level >= 0 {
				curr = list.head[level]
			}
		} else {
			if level <= height {
				prev.next[level] = newNode
				newNode.next[level] = curr
			}
			level--
			if level >= 0 {
				curr = prev.next[level]
			}
		}
	}
}

// Get returns (value, tombstone, found).
func (list *SkipList) Get(key []byte) ([]byte, bool, bool) {
	var prev *node

	list.rwMu.RLock()
	defer list.rwMu.RUnlock()

	curr := list.head[topLevel]

	for level := topLevel; level >= 0; {
		for curr != nil && bytes.Compare(curr.key, key) < 0 {
			prev = curr
			curr = curr.next[level]
		}
		if curr != nil && bytes.Equal(key, curr.key) {
			return curr.value, curr.tombstone, true
		}
		if prev == nil {
			level--
			if level >= 0 {
				curr = list.head[level]
			}
		} else {
			level--
			if level >= 0 {
				curr = prev.next[level]
			}
		}
	}
	return nil, false, false
}

func (list *SkipList) Iterate() []*node {
	var res []*node

	list.rwMu.RLock()
	defer list.rwMu.RUnlock()

	curr := list.head[0]
	for curr != nil {
		res = append(res, curr)
		curr = curr.next[0]
	}
	return res
}
