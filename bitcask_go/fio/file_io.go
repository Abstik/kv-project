package fio

import "os"

// 标准化系统文件
type FileIO struct {
	// go语言提供的文件句柄
	fd *os.File
}

// 创建文件管理器
func NewFileIOManager(fileName string) (*FileIO, error) {
	fd, err := os.OpenFile(
		fileName,                          // 文件名
		os.O_CREATE|os.O_RDWR|os.O_APPEND, // 打开模式（如果文件不存在就创建，以可读可写方式打开，写入数据时追加到文件末尾）
		DataFilePerm,                      // 文件权限（0644）（所有者可读写，群组/其他用户只读）
	)
	if err != nil {
		return nil, err
	}

	return &FileIO{fd: fd}, nil
}

// 实现接口中的文件操作函数，直接调用go语言提供的文件库函数

func (fio *FileIO) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

func (fio *FileIO) Close() error {
	return fio.fd.Close()
}

func (fio *FileIO) Size() (int64, error) {
	stat, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
