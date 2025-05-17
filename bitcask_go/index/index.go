package index

import (
	"bytes"

	"github.com/google/btree"

	"bitcask-go/data"
)

// 抽象索引接口，后续如果想要接入其他数据结构，直接实现这个接口即可
type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
}

// 内存中的键值对，使用BTree实现，结构体实现Less方法，进而实现 btree.Item 接口
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	// 判断 ai.key 是否小于 bi.(*Item).key
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}
