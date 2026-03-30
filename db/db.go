package db

import (
	"sync"

	"github.com/owildev/rangerdb/internal/memtable"
)

type DB struct {
	mu     sync.RWMutex
	mem    *memtable.MemTable
	closed bool
}

func Open(opts Options) (*DB, error) {
	return &DB{
		mem: memtable.New(),
	}, nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.closed = true
	return nil
}

func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDBClosed
	}

	db.mem.Put(key, value)
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrDBClosed
	}

	value, tomb, found := db.mem.Get(key)
	if !found || tomb {
		return nil, ErrKeyNotFound
	}
	return value, nil
}

// Doesn't check if key exists
func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDBClosed
	}

	db.mem.Delete(key)
	return nil
}
