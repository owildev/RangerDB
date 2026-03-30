package db

import "testing"

func TestGetNonExistentKey(t *testing.T) {
	d, _ := Open(Options{})
	defer d.Close()

	_, err := d.Get([]byte("missing"))
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestDeletedKeyReturnsNotFound(t *testing.T) {
	d, _ := Open(Options{})
	defer d.Close()

	d.Put([]byte("key"), []byte("value"))
	d.Delete([]byte("key"))

	_, err := d.Get([]byte("key"))
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound after delete, got %v", err)
	}
}

func TestDeleteNonExistentKey(t *testing.T) {
	d, _ := Open(Options{})
	defer d.Close()

	err := d.Delete([]byte("missing"))
	if err != nil {
		t.Fatalf("expected no error deleting non-existent key, got %v", err)
	}
}
