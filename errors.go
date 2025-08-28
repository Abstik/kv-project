package bitcask_go

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("key为空")
	ErrIndexUpdateFailed      = errors.New("更新索引失败")
	ErrKeyNotFound            = errors.New("key未被找到")
	ErrDataFileNotFound       = errors.New("数据文件未被找到")
	ErrDataDirectoryCorrupted = errors.New("数据文件可能被损坏")
	ErrExceedMaxBatchNum      = errors.New("超出最大批量写入数量")
	ErrMergeIsProgress        = errors.New("正在进行merge")
	ErrDatabaseIsUsing        = errors.New("数据库正在使用")
	ErrMergeRatioUnreached    = errors.New("merge比率未达到")
	ErrNoEnoughSpaceForMerge  = errors.New("merge所需空间不足")
)
