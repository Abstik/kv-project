package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal  LogRecordType = iota // 未被删除
	LogRecordDeleted                      // 已被删除
)

// LogRecord的Header部分：crc(校验值) type(类型) keySize(key大小) valueSize(value大小)
// crc 4字节
// type 1字节
// keySize和valueSize是变长的，最大为5
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

// 对LogRecord编码，返回字节数组及长度
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}

// 对字节数组中的Header解码
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	return nil, 0
}

// 校验有效性
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	return 0
}
