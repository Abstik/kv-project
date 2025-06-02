package redis

import (
	"encoding/binary"
	"math"

	"bitcask-go/utils"
)

const (
	maxMetadataSize   = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32 // 基础元数据的最大值
	extraListMetaSize = binary.MaxVarintLen64 * 2                           // List结构专用的最大值

	initialListMark = math.MaxUint64 / 2 // List结构中head和tail的初始化位置
)

// 元数据
type metadata struct {
	dataType byte   // 数据类型
	expire   int64  // 过期时间
	version  int64  // 版本号
	size     uint32 // 数据量
	head     uint64 // List数据结构专用，队列头
	tail     uint64 // List数据结构专用， 队列尾
}

// 将元数据编码成字节数组
func (md *metadata) encode() []byte {
	var size = maxMetadataSize
	if md.dataType == List {
		size += extraListMetaSize
	}
	buf := make([]byte, size)

	buf[0] = md.dataType
	var index = 1
	index += binary.PutVarint(buf[index:], md.expire)
	index += binary.PutVarint(buf[index:], md.version)
	index += binary.PutVarint(buf[index:], int64(md.size))

	if md.dataType == List {
		index += binary.PutVarint(buf[index:], int64(md.head))
		index += binary.PutVarint(buf[index:], int64(md.tail))
	}

	return buf[:index]
}

// 从字节数组中解码出metadata
func decodeMetadata(buf []byte) *metadata {
	dataType := buf[0]

	var index = 1
	expire, n := binary.Varint(buf[index:])
	index += n
	version, n := binary.Varint(buf[index:])
	index += n
	size, _ := binary.Varint(buf[index:])
	index += n

	var head, tail uint64
	if dataType == List {
		head, n = binary.Uvarint(buf[index:])
		index += n
		tail, _ = binary.Uvarint(buf[index:])
	}

	return &metadata{
		dataType: dataType,
		expire:   expire,
		version:  version,
		size:     uint32(size),
		head:     head,
		tail:     tail,
	}
}

// hash类型数据部分的key
type hashInternalKey struct {
	key     []byte
	version int64 // 8 byte
	filed   []byte
}

// 对hash key编码
func (hk *hashInternalKey) encode() []byte {
	buf := make([]byte, len(hk.key)+len(hk.filed)+8)
	var index = 0

	// 编码key
	copy(buf[index:index+len(hk.key)], hk.key)
	index += len(hk.key)

	// 编码version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(hk.version))
	index += 8

	// 编码field
	copy(buf[index:], hk.filed)

	return buf
}

// set类型数据部分的key
type setInternalKey struct {
	key     []byte
	version int64 // 8 byte
	member  []byte
}

// 对set key编码
func (sk *setInternalKey) encode() []byte {
	// 最后4个字节，存放member size
	buf := make([]byte, len(sk.key)+len(sk.member)+8+4)
	var index = 0

	// 编码key
	copy(buf[index:index+len(sk.key)], sk.key)
	index += len(sk.key)

	// 编码version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(sk.version))
	index += 8

	// 编码member
	copy(buf[index:index+len(sk.member)], sk.member)
	index += len(sk.member)

	// 编码member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(sk.member)))

	return buf
}

type listInternalKey struct {
	key     []byte
	version int64
	index   uint64 // 元素在队列中的位置
}

func (lk *listInternalKey) encode() []byte {
	buf := make([]byte, len(lk.key)+8+8)

	// 编码key
	var index = 0
	copy(buf[index:len(lk.key)], lk.key)
	index += len(lk.key)

	// 编码version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(lk.version))
	index += 8

	// 编码index
	binary.LittleEndian.PutUint64(buf[index:], lk.index)

	return buf
}

type zsetInternalKey struct {
	key     []byte
	version int64
	member  []byte
	score   float64
}

func (zk *zsetInternalKey) encodeWithMember() []byte {
	buf := make([]byte, len(zk.key)+len(zk.member)+8)

	// 编码key
	var index = 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	// 编码version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zk.version))
	index += 8

	// 编码member
	copy(buf[index:], zk.member)

	return buf
}

func (zk *zsetInternalKey) encodeWithScore() []byte {
	scoreBuf := utils.Float64ToBytes(zk.score)
	buf := make([]byte, len(zk.key)+len(zk.member)+len(scoreBuf)+8+4)

	// 编码key
	var index = 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	// 编码version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zk.version))
	index += 8

	// 编码score
	copy(buf[index:index+len(scoreBuf)], scoreBuf)
	index += len(scoreBuf)

	// 编码member
	copy(buf[index:index+len(zk.member)], zk.member)
	index += len(zk.member)

	// 编码member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(zk.member)))

	return buf
}
