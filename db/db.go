package db

import (
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/owildev/rangerdb/internal/memtable"
	"github.com/owildev/rangerdb/internal/sstable"
)

type DB struct {
	closed atomic.Bool
	mem    *memtable.Manager
	opts   *Options
	// next file num for sst's
	nextFileNum atomic.Uint64
	// list of sst's
	ssts    []string
	sstRWMu sync.RWMutex
}

func Open(opts Options, path string) (*DB, error) {
	if opts.MemTableSize == 0 {
		opts.MemTableSize = 64 * 1024 * 1024
	}
	if opts.MaxImmutable == 0 {
		opts.MaxImmutable = 4
	}
	// DB path
	if path == "" {
		opts.Path = "/tmp/rangerdb/"
	} else {
		opts.Path = path
	}

	// Log path
	if err := initLogger(opts.Path + "rangerdb.log"); err != nil {
		return nil, err
	}

	// Read existing sst's
	ssts, maxNum := loadSSTs(opts.Path)

	d := &DB{
		mem:  memtable.NewManager(opts.MemTableSize, opts.MaxImmutable),
		opts: &opts,
		ssts: ssts,
	}
	// next flush starts after the highest existing file number
	d.nextFileNum.Store(maxNum)

	go d.rotateLoop()
	go d.flushLoop()
	return d, nil
}

// loadSSTs scans the db directory for existing sst files. It returns their
// filenames sorted ascending by file number, and the highest number seen.
func loadSSTs(dir string) ([]string, uint64) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		slog.Error("readdir sst files", "err", err)
		return nil, 0
	}

	type sstFile struct {
		num  uint64
		name string
	}
	var files []sstFile
	var maxNum uint64
	for _, e := range entries {
		numStr, ok := strings.CutPrefix(e.Name(), "sst.")
		if !ok {
			continue
		}
		num, err := strconv.ParseUint(numStr, 10, 64)
		if err != nil {
			slog.Warn("skipping unparseable sst file", "name", e.Name())
			continue
		}
		if num > maxNum {
			maxNum = num
		}
		files = append(files, sstFile{num, e.Name()})
	}

	// sort by file number, not name — "sst.10" < "sst.2" lexically
	sort.Slice(files, func(i, j int) bool { return files[i].num < files[j].num })

	ssts := make([]string, len(files))
	for i, fl := range files {
		ssts[i] = fl.name
	}
	return ssts, maxNum
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

	// active + immutable memtables
	value, tomb, found := db.mem.Get(key)
	if found {
		if tomb {
			return nil, ErrKeyNotFound
		}
		return value, nil
	}

	// memtable miss — search ssts newest-to-oldest
	db.sstRWMu.RLock()
	ssts := db.ssts
	db.sstRWMu.RUnlock()

	for i := len(ssts) - 1; i >= 0; i-- {
		value, tomb, found, err := sstable.Read(key, db.opts.Path+ssts[i])
		if err != nil {
			return nil, err
		}
		if found {
			if tomb {
				return nil, ErrKeyNotFound
			}
			return value, nil
		}
	}
	return nil, ErrKeyNotFound
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
			name := "sst." + strconv.FormatUint(db.nextFileNum.Add(1), 10)
			path := db.opts.Path + name
			slog.Info("Flushing", "file", path)
			err := sstable.Write(imm.NewIterator(), path)
			if err != nil {
				slog.Error("sstable write failed", "err", err)
				panic("sstable write failed")
			}

			// add to ssts before FinishFlush removes the immutable,
			// so a concurrent Get always finds the key in one place.
			// store only the filename — opts.Path is prepended on read
			db.sstRWMu.Lock()
			db.ssts = append(db.ssts, name)
			db.sstRWMu.Unlock()

			db.mem.FinishFlush()

		}
	}
}
