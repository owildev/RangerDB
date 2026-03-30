package memtable

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

// 1 key
func TestGetMiss(t *testing.T) {
	sl := NewSkipList()

	_, _, found := sl.Get([]byte("missing"))
	if found {
		t.Fatal("expected not found")
	}
}

// 1 Put, 1 get
func TestInsertAndGet(t *testing.T) {
	sl := NewSkipList()

	sl.Put([]byte("hello"), []byte("world"))

	got, _, found := sl.Get([]byte("hello"))
	if !found {
		t.Fatal("expected found")
	}
	if !bytes.Equal(got, []byte("world")) {
		t.Fatalf("expected 'world', got '%s'", got)
	}
}

// 2 Puts (same key), 1 get
func TestOverwrite(t *testing.T) {
	sl := NewSkipList()

	sl.Put([]byte("key"), []byte("v1"))
	sl.Put([]byte("key"), []byte("v2"))

	got, _, found := sl.Get([]byte("key"))
	if !found {
		t.Fatal("expected found")
	}
	if !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("expected 'v2', got '%s'", got)
	}
}

// small set, mixed order
func TestMultipleKeys(t *testing.T) {
	sl := NewSkipList()

	keys := []string{"banana", "apple", "cherry", "date"}
	for _, k := range keys {
		sl.Put([]byte(k), []byte("val_"+k))
	}

	for _, k := range keys {
		got, _, found := sl.Get([]byte(k))
		if !found {
			t.Fatalf("key %s: expected found", k)
		}
		expected := []byte("val_" + k)
		if !bytes.Equal(got, expected) {
			t.Fatalf("key %s: expected '%s', got '%s'", k, expected, got)
		}
	}
}

// small set, descending order (every Put goes to head)
func TestReverseInsertOrder(t *testing.T) {
	sl := NewSkipList()

	keys := []string{"delta", "cherry", "banana", "apple"}
	for _, k := range keys {
		sl.Put([]byte(k), []byte("val_"+k))
	}

	for _, k := range keys {
		got, _, found := sl.Get([]byte(k))
		if !found {
			t.Fatalf("key %s: expected found", k)
		}
		expected := []byte("val_" + k)
		if !bytes.Equal(got, expected) {
			t.Fatalf("key %s: expected '%s', got '%s'", k, expected, got)
		}
	}
}

// large set, descending order
func TestDescendingInsertOrder(t *testing.T) {
	sl := NewSkipList()
	count := 1000

	for i := count - 1; i >= 0; i-- {
		key := fmt.Appendf(nil, "key-%06d", i)
		sl.Put(key, fmt.Appendf(nil, "val-%06d", i))
	}

	for i := range count {
		key := fmt.Appendf(nil, "key-%06d", i)
		got, _, found := sl.Get(key)
		if !found {
			t.Fatalf("key %d: expected found", i)
		}
		expected := fmt.Appendf(nil, "val-%06d", i)
		if !bytes.Equal(got, expected) {
			t.Fatalf("key %d: expected '%s', got '%s'", i, expected, got)
		}
	}
}

// large set, ascending order
func TestStress(t *testing.T) {
	sl := NewSkipList()
	count := 10000

	for i := range count {
		key := fmt.Appendf(nil, "key-%06d", i)
		sl.Put(key, fmt.Appendf(nil, "val-%06d", i))
	}

	for i := range count {
		key := fmt.Appendf(nil, "key-%06d", i)
		got, _, found := sl.Get(key)
		if !found {
			t.Fatalf("key %d: expected found", i)
		}
		expected := fmt.Appendf(nil, "val-%06d", i)
		if !bytes.Equal(got, expected) {
			t.Fatalf("key %d: expected '%s', got '%s'", i, expected, got)
		}
	}
}

// concurrent reads and writes
func TestConcurrentReadWrite(t *testing.T) {
	sl := NewSkipList()
	var wg sync.WaitGroup

	for i := range 100 {
		sl.Put(fmt.Appendf(nil, "key-%03d", i), fmt.Appendf(nil, "val-%03d", i))
	}

	// writers
	for i := range 10 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := range 100 {
				key := fmt.Appendf(nil, "key-%03d", (i*100+j)%200)
				sl.Put(key, fmt.Appendf(nil, "val-%d-%d", i, j))
			}
		}(i)
	}

	// readers
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 100 {
				sl.Get(fmt.Appendf(nil, "key-%03d", i%100))
			}
		}()
	}

	wg.Wait()
}
