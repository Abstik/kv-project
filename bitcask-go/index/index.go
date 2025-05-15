package index

import (
	"bytes"

	"github.com/google/btree"

	"kv_project/bitcask-go/data"
)

// 抽象索引接口，后续如果想要接入其他数据结构，直接实现这个接口即可
type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
}

// 定义 Item 结构体，为btree中实际存储的内容，实现Less方法，进而实现 btree.Item 接口
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	// 判断 ai.key 是否小于 bi.(*Item).key
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}
