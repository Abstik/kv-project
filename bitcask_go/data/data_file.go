package data

import "bitcask-go/fio"

// 文件后缀
const DataFileNameSuffix = ".data"

// 文件结构体
type DataFile struct {
	FiledId   uint32        // 文件id
	WriteOff  int64         // 文件写入的位置（偏移量）
	IOManager fio.IOManager // io读写管理
}

// 根据文件路径和文件id打开文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	fioManager, err := fio.NewFileIOManager(dirPath)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FiledId:   fileId,
		WriteOff:  0,
		IOManager: fioManager,
	}, nil
}

// 持久化
func (df *DataFile) Sync() error {
	return nil
}

// 写入数据
func (df *DataFile) Write(buf []byte) error {
	return nil
}

// 读取日志记录
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}
