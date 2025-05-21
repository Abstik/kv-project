package bitcask_go

import (
	"bytes"

	"bitcask-go/index"
)

// 索引迭代器（供用户使用）
type Iterator struct {
	indexIter index.Iterator // 索引迭代器
	db        *DB
	options   IteratorOptions
}

// 初始化迭代器
func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator{
		db:        db,
		indexIter: indexIter,
		options:   opts,
	}
}

// 重新回到迭代器的起点，第一个数据
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

// 根据传入的key找到第一个大于等于或小于等于的目标key，从这个key开始遍历
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

// 跳转到下一个key
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

// 是否已经遍历完所有的key，用于退出遍历
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// 当前遍历位置的key数据
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// 当前遍历位置的value数据
func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexIter.Value()
	it.db.mu.Lock()
	defer it.db.mu.RUnlock()
	// 去文件中读取
	return it.db.getValueByPosition(logRecordPos)
}

// 关闭迭代器，释放相应资源
func (it *Iterator) Close() {
	it.indexIter.Close()
}

// 跳过不符合前缀的key
func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}

	for ; it.indexIter.Valid(); it.indexIter.Next() {
		// 迭代器当前遍历到的key
		key := it.indexIter.Key()

		// 判断key的前缀是否匹配
		if prefixLen <= len(key) && bytes.Compare(it.options.Prefix, key[:prefixLen]) == 0 {
			break
		}
	}
}
