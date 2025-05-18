package index

import (
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
