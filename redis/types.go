package redis

import (
	"encoding/binary"
	"errors"
	"time"

	bitcask "bitcask-go"
	"bitcask-go/utils"
)

var (
	ErrWrongTypeOperation = errors.New("wrong Operation against a key holding the wrong kind of value")
)

type redisDataType = byte

const (
	String redisDataType = iota
	Hash
	Set
	List
	ZSet
)

// Redis数据结构服务
type RedisDataStructure struct {
	db *bitcask.DB
}

// 初始化Redis数据结构服务
func NewRedisDataStructure(options bitcask.Options) (*RedisDataStructure, error) {
	db, err := bitcask.Open(options)
	if err != nil {
		return nil, err
	}

	return &RedisDataStructure{db: db}, nil
}

// 关闭服务
func (rds *RedisDataStructure) Close() error {
	return rds.db.Close()
}

// ==============String数据结构==============
func (rds *RedisDataStructure) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	// 编码value：type(数据类型) + expire(过期时间) + payload(原始value)
	buf := make([]byte, binary.MaxVarintLen64+1)

	// 设置数据类型为String
	buf[0] = String

	var index = 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	// 编码过期时间
	index += binary.PutVarint(buf[index:], expire)

	// 编码value
	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)

	// 写入数据
	return rds.db.Put(key, encValue)
}

func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}

	// 解码
	dataType := encValue[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}

	// 解码过期时间
	var index = 1
	expire, n := binary.Varint(encValue[index:])
	index += n

	// 判断是否过期
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}

	// 返回实际value
	return encValue[index:], nil
}

// ==============Hash数据结构==============
func (rds *RedisDataStructure) HSet(key, field, value []byte) (bool, error) {
	// 查找元数据是否存在
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	// 构造Hash数据部分的key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		filed:   field,
	}
	encKey := hk.encode()

	// 查找数据部分的key是否存在（key+field）
	var exist = true
	if _, err = rds.db.Get(encKey); errors.Is(err, bitcask.ErrKeyNotFound) {
		exist = false
	}

	// 初始化原子写，开启事务
	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)

	// 如果数据部分的key不存在，代表此次操作是新增操作，需要增加size
	if !exist {
		// 增加size
		meta.size++
		// 存入实际key和元数据（如果元数据已经写入则更新，如果未写入则新增）
		_ = wb.Put(key, meta.encode())
	}

	// 写入数据部分的key和实际value
	_ = wb.Put(encKey, value)

	// 提交事务
	if err = wb.Commit(); err != nil {
		return false, err
	}

	// 如果key存在，则说明key重复，此时会更新key，但是返回false
	// 如果key不存在，则进行新增，返回true
	return !exist, nil
}

func (rds *RedisDataStructure) HGet(key, field []byte) ([]byte, error) {
	// 查找元数据是否存在
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}

	if meta.size == 0 {
		// 如果size为0，表示此元数据在DB中不存在，为刚初始化的，所以此key也是不存在的，直接返回nil
		return nil, nil
	}

	// 构造数据部分的key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		filed:   field,
	}

	// 根据数据部分的key去查找value
	return rds.db.Get(hk.encode())
}

func (rds *RedisDataStructure) HDel(key, field []byte) (bool, error) {
	// 查找元数据是否存在
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	if meta.size == 0 {
		// 如果size为0，表示此元数据在DB中不存在，为刚初始化的，所以此key也是不存在的，直接返回nil
		return false, nil
	}

	// 构造数据部分的key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		filed:   field,
	}
	encKey := hk.encode()

	// 查找数据部分的key是否存在
	var exist = true
	if _, err = rds.db.Get(encKey); errors.Is(err, bitcask.ErrKeyNotFound) {
		exist = false
	}

	// 如果数据部分的key存在
	if exist {
		// 开启事务
		wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)

		// 因为要删除，所以更新元数据的size
		meta.size--
		_ = wb.Put(key, meta.encode())

		// 删除数据部分的key
		_ = wb.Delete(encKey)

		// 提交事务
		if err = wb.Commit(); err != nil {
			return false, nil
		}
	}

	// 删除key中的filed，如果filed不存在，返回false
	return exist, nil
}

// ==============Set数据结构==============
func (rds *RedisDataStructure) SAdd(key, member []byte) (bool, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	// 构造数据部分的key
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	var ok bool
	if _, err = rds.db.Get(sk.encode()); errors.Is(err, bitcask.ErrKeyNotFound) {
		// 如果key不存在，则新增
		wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
		meta.size++
		// 更新元数据
		_ = wb.Put(key, meta.encode())
		// 更新数据部分
		_ = wb.Put(sk.encode(), nil)
		if err = wb.Commit(); err != nil {
			return false, err
		}

		ok = true
	}

	// 只有key不存在才可以SAdd，如果key已存在，则返回false
	return ok, nil
}

// 查找key下的member是否存在
func (rds *RedisDataStructure) SIsMember(key, member []byte) (bool, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	if meta.size == 0 {
		// 如果元数据原本不存在，刚被初始化
		return false, err
	}

	// 构造数据部分的key
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	// 查找数据部分的key
	_, err = rds.db.Get(sk.encode())
	if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
		return false, err
	}
	if errors.Is(err, bitcask.ErrKeyNotFound) {
		return false, nil
	}

	return true, nil
}

// 删除key下的member
func (rds *RedisDataStructure) SRem(key, member []byte) (bool, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	if meta.size == 0 {
		// 如果元数据原本不存在，刚被初始化
		return false, err
	}

	// 构造数据部分的key
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	if _, err = rds.db.Get(sk.encode()); errors.Is(err, bitcask.ErrKeyNotFound) {
		return false, nil
	}

	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	meta.size--
	// 更新元数据
	_ = wb.Put(key, meta.encode())
	// 删除数据部分的member
	_ = wb.Delete(sk.encode())
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// ==============List数据结构==============
func (rds *RedisDataStructure) LPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, true)
}

func (rds *RedisDataStructure) RPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, false)
}

// 插入数据，返回key下数据的数量
func (rds *RedisDataStructure) pushInner(key, element []byte, isLeft bool) (uint32, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return 0, err
	}

	// 构造数据部分的key
	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}

	if isLeft {
		// 如果是从左边插入
		lk.index = meta.head - 1
	} else {
		// 如果是从右边插入
		lk.index = meta.tail
	}

	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	meta.size++
	if isLeft {
		meta.head--
	} else {
		meta.tail++
	}
	// 更新元数据
	_ = wb.Put(key, meta.encode())
	// 更新数据部分
	_ = wb.Put(lk.encode(), element)
	if err = wb.Commit(); err != nil {
		return 0, err
	}

	return meta.size, nil
}

func (rds *RedisDataStructure) LPop(key []byte) ([]byte, error) {
	return rds.popInner(key, true)
}

func (rds *RedisDataStructure) RPop(key []byte) ([]byte, error) {
	return rds.popInner(key, false)
}

// 删除数据，返回被删除的数据和错误
func (rds *RedisDataStructure) popInner(key []byte, isLeft bool) ([]byte, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	// 构造数据部分的key
	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}

	// 确认数据部分的key的index
	if isLeft {
		// 如果是从左边插入
		lk.index = meta.head
	} else {
		// 如果是从右边插入
		lk.index = meta.tail - 1
	}

	// 根据数据部分的key去查找
	element, err := rds.db.Get(lk.encode())
	if err != nil {
		return nil, err
	}

	// 更新元数据
	meta.size--
	if isLeft {
		meta.head++
	} else {
		meta.tail--
	}
	if err = rds.db.Put(key, meta.encode()); err != nil {
		return nil, err
	}
	return element, nil
}

// ==============ZSet数据结构==============
func (rds *RedisDataStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return false, err
	}

	// 构造数据部分的key
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
		score:   score,
	}

	// 此key下的这个member是否存在
	var exist = true

	// 先根据member key寻找score
	oldScore, err := rds.db.Get(zk.encodeWithMember())
	if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
		return false, err
	}
	if errors.Is(err, bitcask.ErrKeyNotFound) {
		exist = false
	}

	// 如果此key下的这个member存在
	if exist {
		// 如果score相同，表示相同key下的member和score都相同，数据重复，返回false
		if score == utils.Float64FromBytes(oldScore) {
			return false, nil
		}
	}

	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	// 如果此key下的这个member不存在（1.元数据不存在 2.元数据存在，但是数据部分key下的这个member不存在）
	if !exist {
		// 更新元数据（不存在则新增，存在则更新）
		meta.size++
		_ = wb.Put(key, meta.encode())
	}

	// 如果此key下的这个member存在
	if exist {
		// 删除旧的member
		oldKey := &zsetInternalKey{
			key:     key,
			version: meta.version,
			member:  member,
			score:   utils.Float64FromBytes(oldScore),
		}
		_ = wb.Delete(oldKey.encodeWithScore())
	}

	// 将数据部分写入
	_ = wb.Put(zk.encodeWithMember(), utils.Float64ToBytes(score))
	_ = wb.Put(zk.encodeWithScore(), nil)

	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

func (rds *RedisDataStructure) ZScore(key, member []byte) (float64, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return -1, err
	}
	if meta.size == 0 {
		return -1, nil
	}

	// 构造数据部分的key
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	score, err := rds.db.Get(zk.encodeWithMember())
	if err != nil {
		return -1, err
	}

	return utils.Float64FromBytes(score), nil
}

// 查找元数据（根据key和type）
// 如果元数据存在则返回，如果不存在则初始化一个元数据（未写入存储引擎）
func (rds *RedisDataStructure) findMetadata(key []byte, dataType redisDataType) (*metadata, error) {
	metaBuf, err := rds.db.Get(key)
	if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
		// 如果出现错误，并且错误不是key没有找到，则直接返回（key没有找到的错误需要单独处理）
		return nil, err
	}

	var meta *metadata
	var exist = true
	if errors.Is(err, bitcask.ErrKeyNotFound) {
		// 如果错误是key没有找到，后续单独处理
		exist = false
	} else {
		// 如果查询出元数据
		// 解码元数据
		meta = decodeMetadata(metaBuf)

		// 判断数据类型
		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}

		// 判断过期时间
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}

	// 如果key不存在
	if !exist {
		// 构造初始化的元数据，并返回
		meta = &metadata{
			dataType: dataType,
			expire:   0, // 过期时间为0，表示永不过期
			version:  time.Now().UnixNano(),
			size:     0,
		}
	}

	if dataType == List {
		// 如果是List类型，初始化head和tail
		meta.head = initialListMark
		meta.tail = initialListMark
	}

	return meta, nil
}
