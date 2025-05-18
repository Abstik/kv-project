package data

type LogRecordType = byte

const (
	LogRecordNormal  LogRecordType = iota // 未被删除
	LogRecordDeleted                      // 已被删除
)

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
