package db

import "errors"

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrDBClosed    = errors.New("db is closed")
)
