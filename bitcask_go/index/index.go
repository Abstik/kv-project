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

	// 索引中的数据量
	Size() int

	// 获取索引迭代器
	Iterator(reverse bool) Iterator
}

type IndexType = int8

const (
	// Btree索引
	Btree IndexType = iota + 1

	// ART自适应基数树索引
	ART
)

// 初始化索引
func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case Btree:
		return NewBtree()
	case ART:
		// todo
		return nil
	default:
		panic("unsupported index type")
	}
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

// 通用索引迭代器
type Iterator interface {
	// 重新回到迭代器的起点，第一个数据
	Rewind()

	// 根据传入的key找到第一个大于等于或小于等于的目标key，从这个key开始遍历
	Seek(key []byte)

	// 跳转到下一个key
	Next()

	// 是否已经遍历完所有的key，用于退出遍历
	Valid() bool

	// 当前遍历位置的key数据
	Key() []byte

	// 当前遍历位置的value数据
	Value() *data.LogRecordPos

	// 关闭迭代器，释放相应资源
	Close()
}
