package index

import (
	"bytes"
	"sort"
	"sync"

	"github.com/google/btree"

	"bitcask-go/data"
)

// BTree 索引，主要封装了Google的Btree库
type BTree struct {
	// Google的btree.BTree
	tree *btree.BTree

	// 读操作并发安全，写操作需要加锁
	lock *sync.RWMutex
}

// 初始化BTree索引
func NewBtree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := Item{key: key, pos: pos}
	// 加锁
	bt.lock.Lock()
	// 将执行的Item类型插入到Btree中，如果已存在则替换
	bt.tree.ReplaceOrInsert(&it)
	// 解锁
	bt.lock.Unlock()
	return true
}

// 读操作不用加锁
func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	// btreeItem为google中的btree.Item，需要转换为自定义的Item类型
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return false
	}
	return true
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

// 获取索引迭代器
func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBTreeIterator(bt.tree, reverse)
}

// 关闭索引迭代器
func (bt *BTree) Close() error {
	return nil
}

// btree索引迭代器
type btreeIterator struct {
	currIndex int     // 当前遍历的下标位置
	reverse   bool    // 是否是反向遍历
	values    []*Item // key+位置索引信息
}

// 创建btree索引迭代器
func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	// 将内存中所有数据放到数组values中，存放结果为有序的
	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}

	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}

	return &btreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

// 重新回到迭代器的起点，第一个数据
func (bti *btreeIterator) Rewind() {
	bti.currIndex = 0
}

// 根据传入的key找到第一个大于等于或小于等于的目标key，从这个key开始遍历
func (bti *btreeIterator) Seek(key []byte) {
	if bti.reverse {
		// 如果是反向遍历，找第一个小于等于的目标key
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		// 如果是正向遍历，找第一个大于等于的目标key
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}
}

// 跳转到下一个key
func (bti *btreeIterator) Next() {
	bti.currIndex += 1
}

// 是否已经遍历完所有的key，用于退出遍历
func (bti *btreeIterator) Valid() bool {
	return bti.currIndex < len(bti.values)
}

// 当前遍历位置的key数据
func (bti *btreeIterator) Key() []byte {
	return bti.values[bti.currIndex].key
}

// 当前遍历位置的value数据
func (bti *btreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.currIndex].pos
}

// 关闭迭代器，释放相应资源
func (bti *btreeIterator) Close() {
	bti.values = nil
}
