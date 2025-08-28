package fio

const DataFilePerm = 0644

type FileIOType = byte

const (
	// 标准文件IO
	StandardFIO FileIOType = iota

	// 内存文件映射
	MemoryMap
)

// 自定义文件读写接口
type IOManager interface {
	// 从文件指定位置读取数据
	Read([]byte, int64) (int, error)

	// 写入字节数组到文件
	Write([]byte) (int, error)

	// 持久化数据
	Sync() error

	// 关闭文件
	Close() error

	// 获取文件大小
	Size() (int64, error)
}

// 初始化NewIOManager
func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	// 根据文件名创建文件管理器
	switch ioType {
	case StandardFIO:
		return NewFileIOManager(fileName)
	case MemoryMap:
		return NewMMapIOManager(fileName)
	default:
		panic("unsupported io type")
	}
}
