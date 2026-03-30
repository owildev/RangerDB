package memtable

type MemTable struct {
	sl *SkipList
}

func New() *MemTable {
	return &MemTable{sl: NewSkipList()}
}

func (m *MemTable) Put(key, value []byte) {
	m.sl.Put(key, value)
}

func (m *MemTable) Delete(key []byte) {
	m.sl.PutTombstone(key)
}

// Get returns (value, tombstone, found).
func (m *MemTable) Get(key []byte) ([]byte, bool, bool) {
	return m.sl.Get(key)
}
