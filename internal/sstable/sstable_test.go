package sstable

import (
	"bytes"
	"path/filepath"
	"testing"
)

type sliceIter struct {
	items []sliceItem
	idx   int
}
type sliceItem struct {
	key, val []byte
	tomb     bool
}

func (it *sliceIter) Valid() bool     { return it.idx < len(it.items) }
func (it *sliceIter) Next()           { it.idx++ }
func (it *sliceIter) Key() []byte     { return it.items[it.idx].key }
func (it *sliceIter) Value() []byte   { return it.items[it.idx].val }
func (it *sliceIter) Tombstone() bool { return it.items[it.idx].tomb }

func TestWriteAndRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.sst")
	items := []sliceItem{
		{key: []byte("apple"), val: []byte("red")},
		{key: []byte("banana"), val: []byte("yellow")},
		{key: []byte("cherry"), val: []byte("dark-red")},
	}
	if err := Write(&sliceIter{items: items}, path); err != nil {
		t.Fatal(err)
	}
	for _, want := range items {
		got, _, found, err := Read(want.key, path)
		if err != nil {
			t.Fatalf("Read(%s): %v", want.key, err)
		}
		if !found {
			t.Fatalf("Read(%s): not found", want.key)
		}
		if !bytes.Equal(got, want.val) {
			t.Fatalf("Read(%s): got %q, want %q", want.key, got, want.val)
		}
	}
}
