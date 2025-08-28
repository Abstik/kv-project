package redis

import "errors"

// 通用命令

// 根据key删除value
func (rds *RedisDataStructure) Del(key []byte) error {
	return rds.db.Delete(key)
}

// 获取value类型
func (rds *RedisDataStructure) Type(key []byte) (redisDataType, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return 0, err
	}

	if len(encValue) == 0 {
		return 0, errors.New("value is null")
	}

	// 第一个字节就是类型
	return encValue[0], nil
}
