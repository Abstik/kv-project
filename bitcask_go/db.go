package bitcask_go

import (
	"errors"
	"io"
	"os"
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

	// 遍历所有的文件id，处理文件中的记录
	for i, fid := range db.fileIds {
		// 当前遍历到的文件id
		var fileId = uint32(fid)

		// 当前遍历到的文件
		var dataFile *data.DataFile

		// 根据 当前遍历到的文件id 指定 当前遍历到的文件
		if fileId == db.activeFile.FiledId {
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
			if logRecord.Type == data.LogRecordDeleted {
				// 如果文件中的记录被标记为已删除，则删除内存中相应的记录
				// 因为日志文件是追加写入的，所以对key的删除或修改操作，以文件最新记录为准
				// 可能文件开头可能添加了key，文件后续又删除了key，所以遍历到删除操作时要去内存中删除之前添加的key
				db.index.Delete(logRecord.Key)
			} else {
				// 如果文件中记录存在，则新增到内存中
				db.index.Put(logRecord.Key, logRecordPos)
			}

			// 更新文件偏移量，下次循环从新位置开始读取
			offset += size
		}

		// 如果当前是活跃文件，更新下次写入文件的位置
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}
	return nil
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
	// 开启锁
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
		Offset: db.activeFile.WriteOff,
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
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}

	// 写入到当前文件当中
	_, err := db.appendLogRecord(logRecord)
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
