package fio

const DataFilePerm = 0644

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

// 初始化NewIOManager，目前只支持FileIO
func NewIOManager(fileName string) (IOManager, error) {
	// 根据文件名创建文件管理器
	return NewFileIOManager(fileName)
}
