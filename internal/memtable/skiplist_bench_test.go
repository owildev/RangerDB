package memtable

import (
	"fmt"
	"testing"
)

const NumBenchmarkIterations = 100000

func BenchmarkInsert(b *testing.B) {
	sl := NewSkipList()
	i := 0
	for b.Loop() {
		key := fmt.Appendf(nil, "key-%d", i)
		sl.Put(key, []byte("value"))
		i++
	}
}

func BenchmarkGet(b *testing.B) {
	sl := NewSkipList()
	for i := range NumBenchmarkIterations {
		key := fmt.Appendf(nil, "key-%d", i)
		sl.Put(key, []byte("value"))
	}

	b.ResetTimer()
	i := 0
	for b.Loop() {
		key := fmt.Appendf(nil, "key-%d", i%NumBenchmarkIterations)
		sl.Get(key)
		i++
	}
}
