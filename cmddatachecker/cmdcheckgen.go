package cmddatachecker

import (
	"sync"

	"time"

	"github.com/garyburd/redigo/redis"
)

const (
	MAX_TESTDATA   = 10000
	AUTH_PASSWD    = "123456"
	LIST_KEK_LEN   = 64
	LIST_VALUE_LEN = 128
	LIST_DATA_SIZE = 2000
	QUEUE_NUM      = 10
)

var Cmd_map = map[string][]string{
	//"string":    {"set", "get", "mget", "mget32", "mget64", "del"},
	"string":    {"set", "get", "get", "get", "get"}, //, "del"},
	"list":      {"lpush", "lrange", "lpop", "rpop", "ltrim", "del"},
	"hash":      {"hset", "hlen", "hget", "hexists", "hmget", "hkeys", "hgetall", "hdel", "del"},
	"set":       {"sadd", "sismember", "smembers", "sinter", "scard", "spop", "srem", "del"},
	"sortedset": {"zadd", "zrevrange", "zrange", "zrange500", "zrange1000", "zrange2000", "zcount", "zscore", "zrem", "del"},
}

/////////////  interface job///////////////////
type CMDDataChecker interface {
	Init(te int) bool
	Check(wg *sync.WaitGroup, cmd string, num int32, t *time.Timer, authpwd string)
}

////////////////////////////////////////////////
func NewDataChecker(cmd string, client *redis.Pool, id int) CMDDataChecker {
	switch cmd {
	case "string":
		//log.Info("string job")
		return &StringGenerator{Client: client, id: id}
	case "sortedset":
		//log.Info("sortedset job")
		return &SortedSetGenerator{Client: client, id: id}
	case "list":
		//log.Info("sortedset job")
		return &ListGenerator{Client: client, id: id}

	case "hash":
		//log.Info("sortedset job")
		return &HashGenerator{Client: client, id: id}
	case "set":
		//log.Info("sortedset job")
		return &SetGenerator{Client: client, id: id}
	default:
		return nil
	}
}
