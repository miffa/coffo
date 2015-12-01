package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"time"
)

const (
	RedisMaxIdle   = 4000
	RedisMaxActive = 4000
	RedisTimeout   = 1

	INTERRUPT_LOOP    = 1
	UN_INTERRUPT_LOOP = 0

	PIPELINE_NUM = 5
)

func main() {
	ipaddr := "10.3.15.69:19000"
	client := &redis.Pool{
		MaxIdle:     RedisMaxIdle,
		MaxActive:   RedisMaxActive,
		IdleTimeout: RedisTimeout * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", ipaddr)
			if err != nil {
				fmt.Errorf("redis.Dial(\"tcp\", \"%s\") error(%v)", ipaddr, err)
				return nil, err
			}
			return c, err
		},
	}

	conn := client.Get()
	if err := conn.Err(); err != nil {
		fmt.Errorf("f.redisPool.Get() error(%v)", conn.Err())
		return
	}
	defer conn.Close()

        reply, err := conn.Do("SET", "foo", "haha")
        fmt.Printf("reply:%v err:%v\n", reply, err)
        if err != nil {
                fmt.Printf("set error \n")
        }
        if ok, _ := redis.String(reply, err); ok == "OK" {
                fmt.Printf("set is  ok \n")
        } else {
                fmt.Printf("set return is error\n")
        }
        exists, _ := redis.String(conn.Do("GET", "foo"))
        fmt.Printf(" get  foo is %#v\n", exists)

        conn.Do("SET", "foo1", 1)
        conn.Do("SET", "foo2", 2)
        reply, err = conn.Do("MGET", "foo", "foo1", "foo2")
        mydata, _ := redis.Strings(reply, err)
        fmt.Printf(" mget  fooX is %#v\n", reply)
        fmt.Printf(" mget  fooX is %#v\n", mydata)
        reply, err = conn.Do("MGET", "foo1", "foo12", "foo22")
        mydata, _ = redis.Strings(reply, err)
        fmt.Printf(" mget  fooX is %#v\n", reply)
        fmt.Printf(" mget  fooX is %#v\n", mydata)

        reply, err = conn.Do("zadd", 12356, 22222, 1111111)
        reply, err = conn.Do("zadd", 12345, 222222, 222222)
        reply, err = conn.Do("zrange", 12345, 0, 20)
        mydatai, _ := redis.Values(reply, err)
        fmt.Printf(" zrange fooX is %#v\n", reply)
        fmt.Printf(" zrange  fooX is %#v\n", mydatai)
        if len(mydatai) <= 0 {
                fmt.Printf(" zrange ret len %d\n", len(mydatai))
        }
        for _, data := range mydatai {
                idata, err := redis.Int64(data, nil)
                if err != nil {
                        fmt.Printf(" int64 error: %v\n", data)
                }
                fmt.Printf("result: %d\n", idata)
        }
        reply, err = conn.Do("zrange", 123456, 0, 20)
        mydatai, _ = redis.Values(reply, err)
        fmt.Printf("=========== zrange fooX is %#v\n", reply)
        fmt.Printf(" zrange  fooX is %#v\n", mydatai)
        if len(mydatai) <= 0 {
                fmt.Printf(" zrange ret len %d\n", len(mydatai))
        }
        for _, data := range mydatai {
                idata, err := redis.Int64(data, nil)
                if err != nil {
                        fmt.Printf(" int64 error: %v\n", data)
                }
                fmt.Printf("result: %d\n", idata)
        }
}
