package main

import (
	"fmt"
	"log"
	"sync"

	"github.com/tidwall/redcon"

	bitcask "bitcask-go"
	bitcask_redis "bitcask-go/redis"
)

// 兼容Redis协议，使用第三方库

const addr = "127.0.0.1:6380"

type BitcaskServer struct {
	dbs    map[int]*bitcask_redis.RedisDataStructure
	server *redcon.Server
	mu     sync.RWMutex
}

func main() {
	redisDataStructure, err := bitcask_redis.NewRedisDataStructure(bitcask.DefaultOptions)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	// 初始化BitcaskServer
	bitcaskServer := &BitcaskServer{
		dbs: make(map[int]*bitcask_redis.RedisDataStructure),
	}
	bitcaskServer.dbs[0] = redisDataStructure

	// 初始化Redis服务器
	bitcaskServer.server = redcon.NewServer(addr, nil, bitcaskServer.accept, bitcaskServer.close)
	bitcaskServer.listen()

}

func (svr *BitcaskServer) listen() {
	log.Println("bitcask server running, ready to accept connections.")
	_ = svr.server.ListenAndServe()
}

func (svr *BitcaskServer) accept(conn redcon.Conn) bool {
	cli := new(BitcaskClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()
	cli.server = svr
	cli.db = svr.dbs[0]
	// 放入上下文
	conn.SetContext(cli)
	return true
}

func (svr *BitcaskServer) close(conn redcon.Conn, err error) {
	for _, db := range svr.dbs {
		_ = db.Close()
	}

	_ = svr.server.Close()
}
