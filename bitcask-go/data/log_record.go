package data

// 表示一条日志记录在文件中的存储位置
type LogRecordPos struct {
	Fid    uint32 // 文件id，表示将数据存储到了哪个文件当中
	Offset int64  // 偏移，表示将数据存储到了数据文件中的哪个位置
}
