package bitcask_go

// 配置项结构体（封装需要用户自定义的参数）
type Options struct {
	DirPath      string    // 数据库数据文件目录
	DataFileSize int64     // 数据文件的大小（阈值）
	SyncWrites   bool      // 每次写数据是否持久化
	IndexType    IndexType // 索引类型
}

type IndexType = int8

const (
	// Btree索引
	Btree IndexType = iota + 1

	// ART自适应基数树索引
	ART
)
