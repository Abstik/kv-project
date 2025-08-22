package bitcask_go

import (
	"encoding/binary"
	"sync"
	"sync/atomic"

	"bitcask-go/data"
)

// 值为0的事务序列号，标识不是事务提交的记录，将key和nonTransactionSeqNo混合编码构成新的key
const nonTransactionSeqNo uint64 = 0

// 常量key，标识事务已提交（批量写之后，再向数据文件中写入一条新数据，key为txnFinKey，type为data.LogRecordTxnFinished，表示此次事务已提交）
var txnFinKey = []byte("txn-fin")

// 原子批量写入数据，保证原子性
type WriteBatch struct {
	options       WriteBatchOptions
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord // 暂存用户写入的数据，实现一次性批量写入文件
}

// 初始化WriteBatch
func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	// 针对B+树索引做特殊判断
	if db.options.IndexType == BPlusTree && !db.seqNoFileExists && !db.isInitial {
		panic("cannot use write batch, seq no file not exists")
	}

	return &WriteBatch{
		options:       opts,
		mu:            new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// 批量写数据
func (wb *WriteBatch) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 写入暂存区
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
	}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// 删除数据
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 内存中数据不存在，则直接返回无需删除
	if pos := wb.db.index.Get(key); pos == nil {
		// 如果内存中不存在
		if wb.pendingWrites[string(key)] != nil {
			// 如果暂存区还有数据，则删除暂存区的此数据
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	// 暂存logRecord
	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// 提交事务，将暂存区的内容批量写入文件，并更新内存索引
func (wb *WriteBatch) Commit() error {
	if len(wb.pendingWrites) == 0 {
		return nil
	}

	// 检查是否超出最大批量写入数量
	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}

	// 加锁保证事务提交串行化
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 获取当前最新的事务序列号+1（此次批量写，使用这个事务序列号）
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	// 临时缓冲区，存放内存索引的map，用于更新内存
	position := make(map[string]*data.LogRecordPos)

	// 遍历缓冲区，将数据写到到文件中
	for _, record := range wb.pendingWrites {
		// 将key和事务序列号进行编码作为新的key，将整体数据写入文件
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}

		// 暂存进临时缓冲区（此key为原始key），用于批量更新内存
		position[string(record.Key)] = logRecordPos
	}

	// 向数据文件中，写一条标识事务完成的数据
	finishedRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(txnFinKey, seqNo),
		Type: data.LogRecordTxnFinished,
	}
	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	// 根据配置决定是否持久化
	if wb.options.syncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 更新内存索引
	for _, record := range wb.pendingWrites {
		pos := position[string(record.Key)]
		var oldPos *data.LogRecordPos
		if record.Type == data.LogRecordNormal {
			oldPos = wb.db.index.Put(record.Key, pos)
		}
		if record.Type == data.LogRecordDeleted {
			oldPos, _ = wb.db.index.Delete(record.Key)
		}
		if oldPos != nil {
			wb.db.reclaimSize += int64(oldPos.Size)
		}
	}

	// 清空暂存数据
	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

// 编码
// 将事务序列号seqNo和实际key进行编码，拼接成新的字节切片，作为新的key
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	// 将 seqNo 使用变长编码编码到 seq 切片中，返回编码后字节数 n
	n := binary.PutUvarint(seq[:], seqNo)

	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)

	return encKey
}

// 解码
// 将key解码，获取实际的key和事务序列号
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
