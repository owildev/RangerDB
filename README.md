# RangerDB

An LSM-tree key-value store written from scratch in Go — memtable, on-disk
SSTables, and a working read/write path. A learning project.

## Status

**Working**
- [x] Skip-list memtable with active/immutable handoff (atomic swap, writes don't block on flush)
- [x] SSTable on-disk format (sorted entries, index, footer)
- [x] Flush: memtable → SSTable
- [x] Read path: `Get` checks the memtable, then the SSTables
- [x] `Put` / `Get`

**Not yet implemented**
- [ ] Bloom filter per SSTable
- [ ] Sparse block index + LRU block cache
- [ ] Compaction (size-tiered, then leveled)
- [ ] Write-ahead log for crash recovery
- [ ] Deletes (tombstones), range scans

## Build

```
go build ./...
go test ./...
```
