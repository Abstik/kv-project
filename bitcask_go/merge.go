package bitcask_go

import (
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"

	"bitcask-go/data"
	"bitcask-go/utils"
)

const (
	mergeDirName     = "-merge"
	mergeFinishedKey = "merge.finished"
)

// Merge 清理无效数据，生成 Hint 文件
func (db *DB) Merge() error {
	// 如果数据库为空，则直接返回
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()

	// 如果 merge 正在进行当中，则直接返回
	if db.isMerging {
		db.mu.Unlock()
		return ErrMergeIsProgress
	}

	// 查看可以merge的数据是否达到阈值
	totalSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		db.mu.Unlock()
		return err
	}
	if float32(db.reclaimSize)/float32(totalSize) < db.options.DataFileMergeRatio {
		db.mu.Unlock()
		return ErrMergeRatioUnreached
	}

	// 查看磁盘剩余空间容量是否可以容纳merge之后的数据量
	availableDiskSize, err := utils.AvailableDiskSize()
	if err != nil {
		db.mu.Unlock()
		return err
	}
	if uint64(totalSize-db.reclaimSize) >= availableDiskSize {
		return ErrNoEnoughSpaceForMerge
	}

	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	// 持久化当前活跃文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}

	// 将当前活跃文件转换为旧的数据文件
	db.olderFiles[db.activeFile.FileId] = db.activeFile
	// 打开新的活跃文件
	if err := db.setActiveFile(); err != nil {
		db.mu.Unlock()
		return nil
	}

	// 记录最近没有参与 merge 的文件 id
	nonMergeFileId := db.activeFile.FileId

	// 取出所有需要 merge 的文件
	var mergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	db.mu.Unlock()

	// 将merge的文件从小到大进行排序，依次merge
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	// 获取到merge引擎的目录
	mergePath := db.getMergePath()
	// 如果目录存在，说明发生过merge，将其删除掉
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	// 新建一个 merge path 的目录
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	// 在merge目录中，打开一个新的临时 bitcask 实例
	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	mergeOptions.SyncWrites = false
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}

	// 打开hint文件存储索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	// 遍历处理每个数据文件
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// 解析拿到实际的key
			realKey, _ := parseLogRecordKey(logRecord.Key)
			// 根据实际key去内存寻找
			logRecordPos := db.index.Get(realKey)

			// 将文件数据和内存索引比较
			if logRecordPos != nil &&
				logRecordPos.Fid == dataFile.FileId &&
				logRecordPos.Offset == offset { // 如果有效则重写
				// 由于内存中的记录一定有效，所以此记录也有效，可以清除文件中数据的事务序列号标记
				logRecord.Key = logRecordKeyWithSeq(realKey, nonTransactionSeqNo)
				// 重写入merge引擎中的文件中
				_, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}

				// 将当前位置索引写到Hint文件中
				if err = hintFile.WriteHintRecord(realKey, logRecordPos); err != nil {
					return err
				}
			}
			// 增加 offset
			offset += size
		}
	}

	// sync 保证持久化
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}

	// 打开标识merge完成的文件
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}

	// 写标识 merge 完成的文件
	mergeFinRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))), // 未参与merge的文件id，方便下次merge
	}
	encRecord, _ := data.EncodeLogRecord(mergeFinRecord)
	if err := mergeFinishedFile.Write(encRecord); err != nil {
		return err
	}

	// 将标识merge完成的文件持久化
	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}

	return nil
}

// 原目录：tmp/bitcask
// 对应的merge目录：tmp/bitcask-merge
func (db *DB) getMergePath() string {
	// 获取父目录 /tmp
	dir := path.Dir(path.Clean(db.options.DirPath))

	// 获取子目录名称 /bitcask
	base := path.Base(db.options.DirPath)

	// 组合merge文件路径 tmp/bitcask-merge
	return filepath.Join(dir, base+mergeDirName)
}

// 加载merge数据目录
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()

	// 如果目录不存在则直接返回
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// 查找标识merge完成的文件
	var mergeFinished bool      // 标识merge是否完成过
	var mergeFileNames []string // merge过的文件的集合
	// 遍历merge目录下的所有文件
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			// 如果merge完成过
			mergeFinished = true
		}
		// 如果是记录最新事务序列号的文件，则跳过不需要移动
		if entry.Name() == data.SeqNoFileName {
			continue
		}
		// 如果是文件锁文件，则跳过不需要移动
		if entry.Name() == fileLockName {
			continue
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	// 如果merge未完成
	if !mergeFinished {
		return nil
	}

	// 获取最小的未参与merge的文件id
	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return nil
	}

	// 删除旧的DB中的数据文件
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := data.GetDataFileName(db.options.DirPath, fileId)
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	// 将新的数据文件（merge目录下的文件）移动到数据目录（DB引擎的目录）中
	for _, fileName := range mergeFileNames {
		oldPath := filepath.Join(mergePath, fileName)
		newPath := filepath.Join(db.options.DirPath, fileName)
		// 将文件从旧路径移动到新路径
		if err := os.Rename(oldPath, newPath); err != nil {
			return err
		}
	}
	return nil
}

// 获取最小的未参与merge的文件id
func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	// 打开标识merge完成的文件
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}

	// 读取标识merge完成的文件
	record, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}

	// 将标识merge完成的文件的读取到的记录转换为数字
	nonMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}

	return uint32(nonMergeFileId), nil
}

// 从 hint 文件中加载索引
func (db *DB) loadIndexFromHintFile() error {
	// 查看 hint 索引文件是否存在
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	//	打开 hint 索引文件
	hintFile, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}

	// 读取文件中的索引
	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// 解码拿到实际的位置索引
		pos := data.DecodeLogRecordPos(logRecord.Value)
		// 将索引放入内存
		db.index.Put(logRecord.Key, pos)
		offset += size
	}
	return nil
}
