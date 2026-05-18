package db

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestGetNonExistentKey(t *testing.T) {
	d, _ := Open(Options{}, "")
	defer d.Close()

	_, err := d.Get([]byte("missing"))
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestDeletedKeyReturnsNotFound(t *testing.T) {
	d, _ := Open(Options{}, "")
	defer d.Close()

	d.Put([]byte("key"), []byte("value"))
	d.Delete([]byte("key"))

	_, err := d.Get([]byte("key"))
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound after delete, got %v", err)
	}
}

func TestDeleteNonExistentKey(t *testing.T) {
	d, _ := Open(Options{}, "")
	defer d.Close()

	err := d.Delete([]byte("missing"))
	if err != nil {
		t.Fatalf("expected no error deleting non-existent key, got %v", err)
	}
}

func TestFlushToSSTable(t *testing.T) {
	// trailing slash so SST files land inside the dir as "{dir}/1", "{dir}/2"
	dir := "/tmp/rangerdb-test" + "/"
	d, err := Open(Options{MemTableSize: 4 << 20}, dir) // 1 MB memtable
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	val := make([]byte, 1024) // 1 KB value
	for i := range 16000 {    // ~16 MB total, triggers ~4 flushes
		key := fmt.Appendf(nil, "key-%06d", i)
		if err := d.Put(key, val); err != nil {
			t.Fatal(err)
		}
	}

	// poll until at least 3 SSTable files appear or timeout
	deadline := time.Now().Add(10 * time.Second)
	var ssts []os.DirEntry
	for time.Now().Before(deadline) {
		entries, _ := os.ReadDir(dir)
		ssts = ssts[:0]
		for _, e := range entries {
			if e.Name() != "rangerdb.log" {
				ssts = append(ssts, e)
			}
		}
		if len(ssts) >= 3 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if len(ssts) < 3 {
		t.Fatalf("expected at least 3 SSTable files, got %d", len(ssts))
	}
	t.Logf("flushed %d SSTable files", len(ssts))
}

// kv builds a key and a distinct ~1 KB value for index i, so reads can
// verify content rather than just presence.
func kv(i int) (key, val []byte) {
	key = fmt.Appendf(nil, "key-%06d", i)
	val = fmt.Appendf(nil, "val-%06d", i)
	val = append(val, make([]byte, 1024)...)
	return key, val
}

func TestGetFromSSTable(t *testing.T) {
	dir := t.TempDir() + "/"
	d, err := Open(Options{MemTableSize: 1 << 20}, dir) // 1 MB memtable
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	const n = 4000 // ~4 MB, forces several flushes
	for i := range n {
		key, val := kv(i)
		if err := d.Put(key, val); err != nil {
			t.Fatal(err)
		}
	}

	// wait until the flusher has produced at least 2 sst files,
	// so the early keys are guaranteed to be served from disk
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		entries, _ := os.ReadDir(dir)
		count := 0
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), "sst.") {
				count++
			}
		}
		if count >= 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// every key must be readable with the exact value written
	for i := range n {
		key, want := kv(i)
		got, err := d.Get(key)
		if err != nil {
			t.Fatalf("Get(%s): %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get(%s): value mismatch", key)
		}
	}

	// a key that was never written must miss in memtables and all ssts
	if _, err := d.Get([]byte("key-999999")); err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound for absent key, got %v", err)
	}
}
