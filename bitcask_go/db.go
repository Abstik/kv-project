package bitcask_go

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/gofrs/flock"

	"bitcask-go/data"
	"bitcask-go/fio"
	"bitcask-go/index"
	"bitcask-go/utils"
)

const (
	seqNoKey     = "seq.no" // 记录最新事务序列号的文件中的key名
	fileLockName = "flock"  // 文件锁名称
)

// 存储引擎实例
type DB struct {
	options         Options                   // 配置项
	mu              *sync.RWMutex             // 读写锁
	fileIds         []int                     // 文件id集合，只能在加载索引时使用，不能在其他地方更新和使用
	activeFile      *data.DataFile            // 当前活跃的数据文件，可以用于写入
	olderFiles      map[uint32]*data.DataFile // 旧的数据文件，可以用于读取
	index           index.Indexer             // 内存索引
	seqNo           uint64                    // 事务序列号，全局递增（批量操作时为全局递增，无事务时为0）
	isMerging       bool                      // 是否正在merge（同一时刻只允许一个merge）
	seqNoFileExists bool                      // 存储事务序列号的文件是否存在（B+树索引专属）
	isInitial       bool                      // 是否是第一次初始化此数据目录
	fileLock        *flock.Flock              // 文件锁保证多进程之间互斥
	bytesWrite      uint                      // 累计写了多少个字节
	reclaimSize     int64                     // 存储回收的数据文件大小（磁盘中无效数据的大小总量），单位：字节
}

// 存储引擎统计信息
type Stat struct {
	KeyNum          uint  // key 的总数量
	DataFileNum     uint  // 数据文件的数量
	ReclaimableSize int64 // 可以进行 merge 回收的数据量，字节为单位
	DiskSize        int64 // 数据目录所占磁盘空间大小
}

// 打开存储引擎实例（初始化）
func Open(options Options) (*DB, error) {
	// 对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	var isInitial bool

	// 判读数据文件目录是否存在，如果不存在则创建
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 判断当前数据目录是否正在使用
	// 创建一个文件锁
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	// 尝试获取读锁
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	// 获取数据文件目录下的所有文件
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	// 如果文件为空，则说明是第一次初始化此数据目录
	if len(entries) == 0 {
		isInitial = true
	}

	// 初始化DB
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrites),
		isInitial:  isInitial,
		fileLock:   fileLock,
	}

	// 加载merge数据目录
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// 加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// B+树索引，将索引存储在磁盘文件中，启动DB时无需从数据文件加载索引放入内存
	// 如果不是B+树索引，再去加载索引放入内存
	if options.IndexType != BPlusTree {
		// 从hint索引文件中加载索引
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}

		// 从数据文件中加载索引（同时获取到最新事务序列号，赋值给DB中的字段）
		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}

		// 重置IO类型为标准文件IO
		if db.options.MMapAtStartup {
			if err := db.resetIoType(); err != nil {
				return nil, err
			}
		}
	}

	// 从指定文件中取出当前事务序列号（B+树索引专属）
	if options.IndexType == BPlusTree {
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}
		if db.activeFile != nil {
			size, err := db.activeFile.IOManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOff = size
		}
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
	if options.DataFileMergeRatio < 0 || options.DataFileMergeRatio > 1 {
		return errors.New("database data file merge ratio is invalid")
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
		ioType := fio.StandardFIO
		if db.options.MMapAtStartup {
			ioType = fio.MemoryMap
		}
		// 打开数据文件
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid), ioType)
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

// 从数据文件中加载内存索引
func (db *DB) loadIndexFromDataFiles() error {
	if len(db.fileIds) == 0 {
		return nil
	}

	// 由于调用此方法前，已经从hint文件中加载过索引，所以只需要加载没有merge的文件，从其中加载索引
	hasMerge, nonMergeFileId := false, uint32(0)

	mergeFinFileName := db.getMergePath()
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
		var oldPos *data.LogRecordPos
		if typ == data.LogRecordDeleted {
			// 如果文件中的记录被标记为已删除，则删除内存中相应的记录
			// 因为日志文件是追加写入的，所以对key的删除或修改操作，以文件最新记录为准
			// 可能文件开头可能添加了key，文件后续又删除了key，所以遍历到删除操作时要去内存中删除之前添加的key
			oldPos, _ = db.index.Delete(key)
			db.reclaimSize += int64(pos.Size)
		} else {
			// 如果文件中记录存在，则新增到内存中
			oldPos = db.index.Put(key, pos)
		}
		if oldPos != nil {
			db.reclaimSize += int64(oldPos.Size)
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
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset, Size: uint32(size)}

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
	// 释放文件锁
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to unlock the directory, %v", err))
		}
	}()

	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// 关闭索引迭代器（只有B+树需要）
	if err := db.index.Close(); err != nil {
		return err
	}

	// B+树索引启动时不会从数据文件加载索引，所以也拿不到最新的事务序列号
	// 因此要将当前最新事务序列号写入专门文件
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
	}
	encRecord, _ := data.EncodeLogRecord(record)
	if err := seqNoFile.Write(encRecord); err != nil {
		return err
	}
	if err := seqNoFile.Sync(); err != nil {
		return err
	}

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

// 持久化
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
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
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
	db.bytesWrite += uint(size)

	// 自动持久化
	var needSync = db.options.SyncWrites
	if !needSync && db.options.BytesPerSync > 0 && db.bytesWrite >= db.options.BytesPerSync {
		needSync = true
	}
	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		// 清空累计值
		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}
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
		Size:   uint32(size),
	}, nil
}

// 打开新的活跃文件（访问此方法前必须持有互斥锁 ）
func (db *DB) setActiveFile() error {
	var initialField uint32 = 0
	if db.activeFile == nil {
		// 如果当前活跃文件为空则初始化数据文件
		initialField = db.activeFile.FileId + 1
	}

	// 打开新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialField, fio.StandardFIO)
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
		idx++
	}
	return keys
}

// 获取所有key value，并执行用户指定的操作，fn函数为用户传递的参数，表示用户指定的key value操作
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iterator := db.index.Iterator(false)
	defer iterator.Close()
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
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return nil
	}
	db.reclaimSize += int64(pos.Size)

	// 从内存索引中将对应的key删除
	oldPos, ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}
	return nil
}

// 从指定文件中加载最新事务序列号（B+树索引专属）
func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}

	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record, _, err := seqNoFile.ReadLogRecord(0)
	if err != nil {
		return err
	}
	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}

	db.seqNo = seqNo
	db.seqNoFileExists = true

	// 删除文件
	err = os.Remove(fileName)

	return err
}

// 将数据文件的 IO 类型设置为标准文件 IO
func (db *DB) resetIoType() error {
	if db.activeFile == nil {
		return nil
	}

	if err := db.activeFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
		return err
	}
	for _, dataFile := range db.olderFiles {
		if err := dataFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
			return err
		}
	}
	return nil
}

// 返回数据库的相关统计信息
func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var dataFiles = uint(len(db.olderFiles))
	if db.activeFile != nil {
		dataFiles += 1
	}

	dirSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir size : %v", err))
	}
	return &Stat{
		KeyNum:          uint(db.index.Size()),
		DataFileNum:     dataFiles,
		ReclaimableSize: db.reclaimSize,
		DiskSize:        dirSize,
	}
}

// 数据库备份
func (db *DB) Backup(dir string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	// 复制目录到目标路径，并排除文件锁的文件
	return utils.CopyDir(db.options.DirPath, dir, []string{fileLockName})
}
