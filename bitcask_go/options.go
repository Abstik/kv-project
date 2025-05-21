package bitcask_go

import "os"

// 配置项结构体（封装需要用户自定义的参数）
type Options struct {
	DirPath      string    // 数据库数据文件目录
	DataFileSize int64     // 数据文件的大小（阈值）
	SyncWrites   bool      // 每次写数据是否持久化
	IndexType    IndexType // 索引类型
}

// 索引迭代器配置项（供用户调用）
type IteratorOptions struct {
	// 遍历前缀为指定的key，默认为空
	Prefix []byte
	// 是否反向遍历，默认false是正向
	Reverse bool
}

type IndexType = int8

const (
	// Btree索引
	Btree IndexType = iota + 1

	// ART自适应基数树索引
	ART
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrites:   false,
}

var DefaultIteratorOptions = IteratorOptions{
	nil,
	false,
}
