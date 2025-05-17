package bitcask_go

import (
	"sync"

	"bitcask-go/data"
	"bitcask-go/index"
)

// 存储引擎实例
type DB struct {
	options    Options                   //  配置项
	mu         *sync.RWMutex             // 读写锁
	activeFile *data.DataFile            // 当前活跃的数据文件，可以用于写入
	olderFiles map[uint32]*data.DataFile // 旧的数据文件，可以用于读取
	index      index.Indexer             // 内存索引
}

// 将键值对写入文件
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造日志记录结构体（向文件中写入的是一条日志记录）
	logRecord := data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 将日志记录写入文件
	pos, err := db.appendLogRecord(&logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引
	if ok := db.index.Put(logRecord.Key, pos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// 将日志记录结构体写入文件
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

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
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 将当前活跃文件转换为旧的数据文件
		db.olderFiles[db.activeFile.FiledId] = db.activeFile

		// 打开新的数据文件
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	writeOff := db.activeFile.WriteOff
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
		Fid:    db.activeFile.FiledId,
		Offset: writeOff,
	}, nil
}

// 设置当前活跃文件（访问此方法前必须持有互斥锁 ）
func (db *DB) setActiveFile() error {
	var initialField uint32 = 0
	if db.activeFile == nil {
		// 如果当前活跃文件为空则初始化数据文件
		initialField = db.activeFile.FiledId + 1
	}

	// 打开新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialField)
	if err != nil {
		return err
	}

	db.activeFile = dataFile
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

	// 根据文件id找到对应的数据文件
	var dataFile *data.DataFile // 要访问的目标数据文件
	if db.activeFile.FiledId == logRecordPos.Fid {
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
	logRecord, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	// 判断是否为删除记录
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, nil
}
