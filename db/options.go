package db

type Options struct {
	// Path is the directory where the database files will be stored.
	// Currently unused until persistence is added.
	Path         string
	MemTableSize int64 // flush threshold, default 64MB
	MaxImmutable int   // max immutable memtables before stalling, default 4

}
