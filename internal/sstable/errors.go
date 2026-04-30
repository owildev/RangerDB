package sstable

import "errors"

var (
	ErrDBCorrupted = errors.New("sst is corrupted")
)
