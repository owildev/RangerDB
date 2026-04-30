package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"log/slog"
	"os"
)

const (
	version    = 1
	headerSize = 16 // magic(8) + version(4) + flags(4)
	footerSize = 20 // index_offset(8) + index_size(4) + magic(8)

	keyLen   = 4
	valLen   = 4
	tombSize = 1
)

var (
	magicBytes = [8]byte{'R', 'A', 'N', 'G', 'E', 'R', 'D', 'B'}
)

type header struct {
	Magic   [8]byte
	Version uint32
	Flags   uint32
}

type footer struct {
	Magic       [8]byte
	IndexOffset int64
	IndexSize   uint32
}

// indexEntry holds a key and its byte offset in the file.
type indexEntry struct {
	key    []byte
	offset int64
}

type entry struct {
	keylen    uint32
	key       []byte
	vallen    uint32
	val       []byte
	tombstone bool
}

// Iterator is satisfied by any sorted key-value iterator.
// *memtable.Iterator satisfies this without sstable importing memtable.
type Iterator interface {
	Valid() bool
	Next()
	Key() []byte
	Value() []byte
	Tombstone() bool
}

// Writer writes a memtable iterator to an SSTable file.
type Writer struct {
	f     *os.File
	w     io.Writer
	index []indexEntry
	pos   uint64 // current byte offset in file
}

// Reader reads an SSTable file.
type Reader struct {
	f     *os.File
	index []indexEntry
}

func Write(it Iterator, path string) error {
	var offset int64
	index := make([]indexEntry, 0, 8)
	f, err := os.Create(path)
	if err != nil {
		slog.Error("sst file create", "err", err)
		return err
	}
	defer f.Close()

	buf := bufio.NewWriterSize(f, 1<<20) // 1MB buffersize

	// Write the header
	hdr := header{Magic: magicBytes, Version: version, Flags: 0}
	err = binary.Write(buf, binary.LittleEndian, hdr)
	if err != nil {
		slog.Error("sst file header write", "err", err)
		return err
	}

	offset = headerSize // unsafe.Sizeof(hdr) would also add padding bytes if the struct get padded by the compiler

	for ; it.Valid(); it.Next() {
		key := it.Key()
		val := it.Value()
		tomb := it.Tombstone()

		// Write the entry
		binary.Write(buf, binary.LittleEndian, uint32(len(key)))
		buf.Write(key)
		binary.Write(buf, binary.LittleEndian, uint32(len(val)))
		buf.Write(val)
		binary.Write(buf, binary.LittleEndian, tomb)

		// Write the offset into the index slice
		index = append(index, indexEntry{key, offset})

		// Increment offset
		offset += int64(keyLen + valLen + len(key) + len(val) + tombSize)

	}

	footer := footer{Magic: magicBytes, IndexOffset: offset}

	for _, e := range index {
		binary.Write(buf, binary.LittleEndian, uint32(len(e.key)))
		buf.Write(e.key)
		binary.Write(buf, binary.LittleEndian, e.offset)
		offset += int64(keyLen + len(e.key) + 8)
	}

	footer.IndexSize = uint32(offset - footer.IndexOffset)
	err = binary.Write(buf, binary.LittleEndian, footer)
	if err != nil {
		slog.Error("sst file footer write", "err", err)
		return err
	}

	err = buf.Flush()
	if err != nil {
		slog.Error("sst buf flush", "err", err)
		return err
	}
	err = f.Sync()
	if err != nil {
		slog.Error("sst file sync", "err", err)
		return err
	}
	slog.Info("sst file written", "file", path)

	return nil
}

// Returns val, tombstone, found
func Read(key []byte, path string) ([]byte, bool, bool, error) {
	f, err := os.Open(path)
	if err != nil {
		slog.Error("sst file open", "key", key, "err", err)
		return nil, false, false, err
	}

	_, err = f.Seek(-footerSize, io.SeekEnd)
	if err != nil {
		slog.Error("sst file seek", "key", key, "err", err)
		return nil, false, false, err
	}

	var ftr footer
	err = binary.Read(f, binary.LittleEndian, ftr)
	if err != nil {
		slog.Error("sst file footer read", "key", key, "err", err)
		return nil, false, false, err
	}

	if ftr.Magic != magicBytes {
		return nil, false, false, ErrDBCorrupted
	}

	f.Seek(ftr.IndexOffset, io.SeekStart)

	var index []indexEntry
	r := io.LimitReader(f, int64(ftr.IndexSize))
	for {
		var keyLen uint32
		err := binary.Read(r, binary.LittleEndian, &keyLen)
		if err != nil {
			if err == io.EOF {
				break
			}
			slog.Error("sst index read", "err", err)
		}
		key := make([]byte, keyLen)
		io.ReadFull(r, key)
		var offset int64
		binary.Read(r, binary.LittleEndian, &offset)
		index = append(index, indexEntry{key: key, offset: offset})
	}

	idx, found := binSearch(index, key)
	if !found {
		return nil, false, false, nil
	}
	off := index[idx].offset

	f.Seek(off, io.SeekStart)

	return nil, false, false, nil
}

// Searches for the key in the index
func binSearch(index []indexEntry, key []byte) (int, bool) {
	low, mid, high := 0, 0, len(index)-1

	for low < high {
		mid = (low + high) / 2
		if bytes.Compare(key, index[mid].key) < 0 {
			high = mid - 1
			continue
		} else if bytes.Compare(key, index[mid].key) > 0 {
			low = mid + 1
			continue
		} else {
			return mid, true
		}
	}
	return -1, false
}
