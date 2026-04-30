package db

import (
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/owildev/rangerdb/internal/memtable"
	"github.com/owildev/rangerdb/internal/sstable"
)

type DB struct {
	closed      atomic.Bool
	mem         *memtable.Manager
	opts        *Options
	nextFileNum atomic.Uint64
}

func Open(opts Options, path string) (*DB, error) {
	if opts.MemTableSize == 0 {
		opts.MemTableSize = 64 * 1024 * 1024
	}
	if opts.MaxImmutable == 0 {
		opts.MaxImmutable = 4
	}
	if path == "" {
		opts.Path = "/tmp/rangerdb/"
	} else {
		opts.Path = path
	}

	if err := initLogger(opts.Path + "rangerdb.log"); err != nil {
		return nil, err
	}

	d := &DB{
		mem:  memtable.NewManager(opts.MemTableSize, opts.MaxImmutable),
		opts: &opts,
	}

	go d.rotateLoop()
	go d.flushLoop()
	return d, nil
}

func initLogger(path string) error {
	var out *os.File
	if path == "" {
		out = os.Stderr
	} else {
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return err
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		out = f
	}
	handler := slog.NewTextHandler(out, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	slog.SetDefault(slog.New(handler))
	return nil
}

func (db *DB) Close() error {
	slog.Info("closing db")
	db.closed.Store(true)
	db.forceRotate()
	db.waitDrain()
	db.mem.Close()
	return nil
}

func (db *DB) Put(key, value []byte) error {
	if db.closed.Load() {
		return ErrDBClosed
	}
	db.mem.Put(key, value)
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	if db.closed.Load() {
		return nil, ErrDBClosed
	}
	value, tomb, found := db.mem.Get(key)
	if !found || tomb {
		return nil, ErrKeyNotFound
	}
	return value, nil
}

func (db *DB) Delete(key []byte) error {
	if db.closed.Load() {
		return ErrDBClosed
	}
	db.mem.Delete(key)
	return nil
}

// Called to flush memtable during db close
func (db *DB) forceRotate() {
	slog.Info("force rotating memtable")
	db.mem.Rotate()
}

// wait for memtables to be flushed during close
func (db *DB) waitDrain() {
	for db.mem.NumImmutables() != 0 {
		time.Sleep(100 * time.Millisecond)
	}
	slog.Info("memtables drained")
}

// rotateLoop listens for rotate signals and swaps the active memtable.
func (db *DB) rotateLoop() {
	for range db.mem.RotateCh() {
		// Size check is needed to avoid rotating on
		// backed up signals for the same rotate
		if db.mem.ActiveSize() >= db.opts.MemTableSize {
			db.mem.Rotate()
		}

	}
}

// flushLoop listens for flush signals and drains the immutable queue.
func (db *DB) flushLoop() {
	for range db.mem.FlushCh() {
		for {
			imm := db.mem.ImmutableFront()
			if imm == nil {
				break
			}
			path := db.opts.Path + "sst." + strconv.FormatUint(db.nextFileNum.Add(1), 10)
			slog.Info("Flushing", "file", path)
			err := sstable.Write(imm.NewIterator(), path)
			if err != nil {
				slog.Error("sstable write failed", "err", err)
				panic("sstable write failed")
			}
			db.mem.FinishFlush()

		}
	}
}
