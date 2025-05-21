package bitcask_go

import (
	"sync"

	"bitcask-go/data"
)

// 原子批量写入数据，保证原子性
type WriteBatch struct {
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord
}
