package data

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"

	"bitcask-go/fio"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted") // crc值校验失败
)

// 文件后缀
const DataFileNameSuffix = ".data"

// 文件结构体
type DataFile struct {
	FiledId   uint32        // 文件id
	WriteOff  int64         // 文件写入的位置（偏移量）
	IOManager fio.IOManager // io读写管理
}

// 根据文件路径和文件id打开文件（返回DataFile文件结构体，可以对此文件进行管理）
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)

	// 初始化IOManager管理器接口
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}

	return &DataFile{
		FiledId:   fileId,
		WriteOff:  0,
		IOManager: ioManager,
	}, nil
}

// 读取日志文件记录（返回日志记录、长度(用于更新文件偏移量)、错误）
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	// 获取文件大小
	fileSize, err := df.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}

	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		// 如果读取部分超出了文件大小（由于Header是可变长的），则读取文件范围内的部分
		headerBytes = fileSize - offset
	}

	// 在文件中读取Header
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	// 对Header进行解码
	header, headerSize := decodeLogRecordHeader(headerBuf)
	// 以下两个条件表示：读到了文件末尾
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	// 取出对应的key和value的长度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)

	// logRecord为函数返回的日志记录
	logRecord := &LogRecord{Type: header.recordType}

	// 读取key和value
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}

		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	// 校验数据有效性
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}

	// 日志记录总长度
	var recordSize = headerSize + keySize + valueSize

	return logRecord, recordSize, nil
}

// 写入数据
func (df *DataFile) Write(buf []byte) error {
	n, err := df.IOManager.Write(buf)
	if err != nil {
		return err
	}

	// 更新写入文件位置
	df.WriteOff += int64(n)
	return nil
}

// 持久化
func (df *DataFile) Sync() error {
	return df.IOManager.Sync()
}

// 读取文件：从偏移量offset开始读取n个字节
func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IOManager.Read(b, offset)
	return
}
