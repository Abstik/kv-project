package bitcask_go

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"bitcask-go/data"
	"bitcask-go/index"
)

// 存储引擎实例
type DB struct {
	options    Options                   // 配置项
	mu         *sync.RWMutex             // 读写锁
	fileIds    []int                     // 文件id集合，只能在加载索引时使用，不能在其他地方更新和使用
	activeFile *data.DataFile            // 当前活跃的数据文件，可以用于写入
	olderFiles map[uint32]*data.DataFile // 旧的数据文件，可以用于读取
	index      index.Indexer             // 内存索引
	seqNo      uint64                    // 事务序列号，全局递增（批量操作时为全局递增，无事务时为0）
	isMerging  bool                      // 是否正在merge（同一时刻只允许一个merge）
}

// 打开存储引擎实例（初始化）
func Open(options Options) (*DB, error) {
	// 对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// 判读数据文件目录是否存在，如果不存在则创建
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 初始化DB
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
	}

	// 加载merge数据目录
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// 从hint中加载索引
	if err := db.loadIndexFromHintFile(); err != nil {
		return nil, err
	}

	// 加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 从数据文件中加载索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}
	return db, nil
}

// 检查配置项（用户自定义参数）
func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size is invalid")
	}
	return nil
}

// 从磁盘加载数据文件
func (db *DB) loadDataFiles() error {
	// 取出文件目录中所有的文件
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	// 文件id集合
	var fileIds []int

	// 遍历目录中所有文件，找到所有以.data结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			// 如果是以.data（自定义的扩展名）结尾的文件，获取文件id
			splitNames := strings.Split(entry.Name(), data.DataFileNameSuffix)
			fileId, err := strconv.Atoi(splitNames[0])
			if err != nil {
				// 数据目录可能损坏
				return ErrDataDirectoryCorrupted
			}
			// 将文件id加入集合
			fileIds = append(fileIds, fileId)
		}
	}

	// 对文件id排序，从小到大依次加载
	// 文件id是递增的，写入也是追加写入，最大的文件id即为当前活跃文件
	sort.Ints(fileIds)
	db.fileIds = fileIds

	// 遍历每个文件id，打开对应的数据文件，存入DB的当前活跃文件和旧文件集合中
	for i, fid := range fileIds {
		// 打开数据文件
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}

		if i == len(fileIds)-1 {
			// 如果是最后一个文件，id是最大的，是当前活跃文件
			db.activeFile = dataFile
		} else {
			db.olderFiles[uint32(fid)] = dataFile
		}
	}

	return nil
}

// 从数据文件中加载索引
func (db *DB) loadIndexFromDataFiles() error {
	if len(db.fileIds) == 0 {
		return nil
	}

	// 由于调用此方法前，已经从hint文件中加载过索引，所以只需要加载没有merge的文件，从其中加载索引
	hasMerge, nonMergeFileId := false, uint32(0)

	mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	// 判断标识merge完成的文件是否存在
	if _, err := os.Stat(mergeFinFileName); err == nil {
		// 获取未merge的文件id
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}

	// 定义更新内存索引的函数
	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var ok bool
		if typ == data.LogRecordDeleted {
			// 如果文件中的记录被标记为已删除，则删除内存中相应的记录
			// 因为日志文件是追加写入的，所以对key的删除或修改操作，以文件最新记录为准
			// 可能文件开头可能添加了key，文件后续又删除了key，所以遍历到删除操作时要去内存中删除之前添加的key
			ok = db.index.Delete(key)
		} else {
			// 如果文件中记录存在，则新增到内存中
			ok = db.index.Put(key, pos)
		}
		if !ok {
			panic("failed to update index at startup")
		}
	}

	// 暂存事务数据（日志中可能有多条记录是属于用一个事务的，当遍历到事务结束标识才能将这些记录统一更新进内存索引）
	// map的key为事务id，value为事务中的所有提交记录
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo = nonTransactionSeqNo

	// 遍历所有的文件id，处理文件中的记录
	for i, fid := range db.fileIds {
		// 当前遍历到的文件id
		var fileId = uint32(fid)

		// 如果之前发生过merge并且当前遍历到的文件id小于未merge的文件id，则当前文件已经从hint中加载过索引，可以直接跳过
		if hasMerge && fileId < nonMergeFileId {
			continue
		}

		// 当前遍历到的文件
		var dataFile *data.DataFile

		// 根据 当前遍历到的文件id 指定 当前遍历到的文件
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		// 读取文件中的记录
		var offset int64 = 0
		for {
			// 根据偏移量读取当前文件的一条日志记录
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					// 如果文件已读到末尾，跳出循环
					break
				}
				return err
			}

			// 构造内存索引并保存进内存
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset}

			// 解析key，拿到事务序列号
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo { // 如果不是事务提交的记录，则直接更新内存
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else { // 如果是事务提交的记录
				// 遍历到文件中标识事务完成的记录，则可以更新到内存索引中
				if logRecord.Type == data.LogRecordTxnFinished {
					// 遍历事务暂存集合的所有记录，逐个更新到内存中
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, logRecordPos)
					}

					// 清空事务暂存集合
					delete(transactionRecords, seqNo)
				} else {
					// 如果没有遍历到事务完成的记录，则将当前事务记录暂存
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			// 更新事务序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			// 更新文件偏移量，下次循环从新位置开始读取
			offset += size
		}

		// 如果当前是活跃文件，更新下次写入文件的位置
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	// 更新事务序列号
	db.seqNo = currentSeqNo
	return nil
}

// 关闭数据库
func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// 关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	// 关闭旧的数据文件
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

// 将键值对写入文件
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造日志记录结构体（向文件中写入的是一条日志记录）
	logRecord := data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo), // 将实际key和非事务序列号一起编码，作为新的key
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 将日志记录写入文件
	pos, err := db.appendLogRecordWithLock(&logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引
	if ok := db.index.Put(logRecord.Key, pos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// 将日志记录写入文件（加锁版）
func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	// 开启锁
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// 将日志记录结构体写入文件（不加锁版）
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	// 判断当前活跃文件是否存在，因为数据库没有写入时没有文件生成
	if db.activeFile == nil {
		// 如果为空则初始化数据文件
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	// 写入数据编码
	encRecord, size := data.EncodeLogRecord(logRecord)

	// 如果写入的数据超过活跃文件的阈值，则关闭活跃文件并打开新的文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		// 将当前活跃文件持久化
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 将当前活跃文件转换为旧的数据文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// 打开新的数据文件
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	// 根据用户配置决定是否持久化
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	// 构造内存记录并返回
	return &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: db.activeFile.WriteOff,
	}, nil
}

// 设置当前活跃文件（访问此方法前必须持有互斥锁 ）
func (db *DB) setActiveFile() error {
	var initialField uint32 = 0
	if db.activeFile == nil {
		// 如果当前活跃文件为空则初始化数据文件
		initialField = db.activeFile.FileId + 1
	}

	// 打开新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialField)
	if err != nil {
		return err
	}

	db.activeFile = dataFile
	return nil
}

// 获取所有key的集合
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		// todo idx递增
	}
	return keys
}

// 获取所有key value，并执行用户指定的操作，fn函数为用户传递的参数，表示用户指定的key value操作
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iterator := db.index.Iterator(false)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}

		// 传入key value进行用户指定的操作
		if !fn(iterator.Key(), value) {
			// 如果出错，则跳出循环终止操作
			break
		}
	}

	return nil
}

// 根据key读取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	// 读取时加读写锁
	db.mu.RLock()
	defer db.mu.RUnlock()

	// 判断key的有效性
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 从内存索引中获取对应的位置信息
	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	// 从数据文件中获取value
	return db.getValueByPosition(logRecordPos)
}

// 根据索引信息获取对应的value（使用此方法前加锁）
func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
	// 根据文件id找到对应的数据文件
	var dataFile *data.DataFile // 要访问的目标数据文件
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
		return nil, ErrDataFileNotFound
	}

	// 如果目标数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 去目标文件读取数据
	// 由于内存索引保存的一定是此key对应的最新日志文件的offset，所以读取到的一定是最新的记录
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	// 判断是否为删除记录
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, nil
}

// 根据key删除对应的数据
func (db *DB) Delete(key []byte) error {
	// 判断key的有效性
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 检查key是否存在
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// 构造文件记录，标记为已删除
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}

	// 写入到当前文件当中
	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return nil
	}

	// 从内存索引中将对应的key删除
	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}
