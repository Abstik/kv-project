package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal      LogRecordType = iota // 未被删除
	LogRecordDeleted                          // 已被删除
	LogRecordTxnFinished                      // 已被提交（批量写之后，再向数据文件中写入一条新数据，Type为LogRecordTxnFinished，表示此次事务已提交）
)

// LogRecord的Header部分：crc(校验值) type(类型) keySize(key大小) valueSize(value大小)
// crc 4字节
// type 1字节
// keySize和valueSize是变长的，最大为5字节
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5 // Header的最大大小

// LogRecord的头部信息
type logRecordHeader struct {
	crc        uint32        // 校验值
	recordType LogRecordType // 标识LogRecord的类型
	keySize    uint32        // key的长度
	valueSize  uint32        //  value的长度
}

// 文件中的记录（因为数据文件的数据是追加写入，类似日志格式，所以叫日志）
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType // 数据删除时的墓碑值
}

// 内存中的记录，表示key对应的value值
type LogRecordPos struct {
	Fid    uint32 // 文件id，表示将数据存储到了哪个文件当中
	Offset int64  // 偏移，表示将数据存储到了数据文件中的哪个位置
}

// 暂存的事务相关数据
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// 对LogRecord编码，返回字节数组及长度
// LogRecord的Header部分：crc(校验值) type(类型) keySize(key大小) valueSize(value大小)
// crc 4字节
// type 1字节
// keySize和valueSize是变长的，最大为5
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	// 初始化header的字节数组
	header := make([]byte, maxLogRecordHeaderSize)

	// 第五个字节存储Type
	header[4] = logRecord.Type
	var index = 5

	// 第五个字节后，存储keySize和valueSize
	// 使用变长类型节省空间
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))
	// 此时index的值为header的长度

	// size为日志记录整体长度
	var size = index + len(logRecord.Key) + len(logRecord.Value)

	// 初始化整体日志记录的数组
	encBytes := make([]byte, size)
	// 将header拷贝
	copy(encBytes[:index], header[:index])
	// 将key和value拷贝进字节数组
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	// 对整个LogRecord进行数据校验
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size)
}

// 对字节数组中的Header解码
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	var index = 5
	// 取出实际的key和value
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

// 校验有效性
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}
