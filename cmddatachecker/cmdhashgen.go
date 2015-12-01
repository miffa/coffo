package cmddatachecker

import (
	"coffo/reporter"
	"strconv"
	"sync"
	"time"

	log "code.google.com/p/log4go"
	"github.com/garyburd/redigo/redis"
)

/////////////////////////////////////////////////////////
//           hash
/////////////////////////////////////////////////////////

type DataItem struct {
	name  string
	value int64
}

type HashDataItem struct {
	key   string
	names []*DataItem
}

type HashGenerator struct {
	data_list []*HashDataItem
	Client    *redis.Pool
	id        int
}

func (self *HashGenerator) Init(count int) bool {
	//start := time.Now().UnixNano() //debug
	var name_list = [4]string{"cpu123456789012345678901234567890", "mem123456789012345678901234567890", "disk123456789012345678901234567890", "network123456789012345678901234567890"}
	var value_list = [4][5]int64{{386, 8664, 486, 586, 786},
		{8, 16, 32, 64, 128},
		{500, 1000, 2000, 16000, 32000},
		{2, 50, 100, 500, 1000}}

	self.data_list = make([]*HashDataItem, count)
	for testquantity := 0; testquantity < count; testquantity++ {

		key := MyDataGen.RandomStringKey(STRING_KEY_LEN)
		index := MyIndex.GetUid()
		key = key + strconv.FormatInt(index, 10)
		hashdataitem := make([]*DataItem, 4)
		onehashdata := &HashDataItem{key, hashdataitem} //one data
		for i := 0; i < 4; i++ {                        //name list
			dataitem := &DataItem{name_list[i], value_list[i][testquantity%5]}
			hashdataitem[i] = dataitem
		}
		self.data_list[testquantity] = onehashdata
	}
	//end := time.Now().UnixNano()
	//输出执行时间，单位为毫秒。
	//log.Info("HashGenerator init use time %d ms", (end-start)/1000000)
	//log.Info("here has [%d] args in it, MAX_TESTDATA[%d]\n", len(self.data_list), MAX_TESTDATA)
	return true
}

func (self *HashGenerator) Check(wg *sync.WaitGroup, cmd string, reqnum int32, mytime *time.Timer, authpasswd string) {

	conn := self.Client.Get()
	if err := conn.Err(); err != nil {
		log.Error("f.redisPool.Get() error(%v)", conn.Err())
		return
	}
	defer conn.Close()
	defer wg.Done()
	if authpasswd != "" {
		reply, err := conn.Do("auth", authpasswd)
		if err != nil {
			log.Error("codis auth error, exit testing")
			return
		}

		if ok, _ := redis.String(reply, err); ok != "OK" {
			log.Error("codis auth not ok, exit testing")
			return
		}
	}
	//	"hash":      {"hset", "hexists",hlen", "hget", "hmget", "hgetall", "hdel", "del"},
	switch cmd {
	case "hset": //key name value : int 0 1
		self.hset(conn, reqnum, mytime)
	case "hexists": //key name : int 0 1
		self.hexists(conn, reqnum, mytime)
	case "hget": //key name : value o nil
		self.hexists(conn, reqnum, mytime)
	case "hlen": //key : int 0~n
		self.hlen(conn, reqnum, mytime)
	case "hkeys": //key : int 0~n
		self.hkeys(conn, reqnum, mytime)
	case "hmget": // key name1 name 2 name 3 name 4
		self.hmget(conn, reqnum, mytime)
	case "hgetall":
		self.hgetall(conn, reqnum, mytime)
	case "hdel":
		self.hdel(conn, reqnum, mytime)
	case "del":
		self.del(conn, reqnum, mytime)
	}
}

// key name value
func (self *HashGenerator) hset(conn redis.Conn, reqnum int32, mytime *time.Timer) {
	length := int32(len(self.data_list))

	var i int32 = 0
	for ; i < reqnum; i++ {
		values := self.data_list[i%length]
		key := values.key
		for _, data := range values.names {

			select {
			case <-mytime.C:
				return
			default:
				reporter.Resultdata.AddSendQuantity()
				_, err := redis.Int(conn.Do("hset", key, data.name, data.value))
				if err != nil {
					log.Error("redis lpush   failed: [%s:%s:%d],err %v", key, data.name, data.value, err)
					reporter.Resultdata.AddFailQuantity()

				} else {
					reporter.Resultdata.AddSuccQuantity()
					//log.Error("redis  lpush  failed: %v", values)
				}
			}
		}
	}
}

func (self *HashGenerator) hget(conn redis.Conn, reqnum int32, mytime *time.Timer) {
	length := int32(len(self.data_list))

	var i, j int32 = 0, 0
	for ; j < reqnum; i++ {
		values := self.data_list[i%length]
		for _, data := range values.names {
			if j > reqnum {
				break
			}
			j++

			select {
			case <-mytime.C:
				return
			default:
				reporter.Resultdata.AddSendQuantity()
				reply, err := redis.Int(conn.Do("hget", values.key, data.name))
				if err != nil {
					log.Error("redis hget  failed: [%s:%s],err %v", values.key, data.name, err)
					reporter.Resultdata.AddFailQuantity()

				} else {
					//reporter.Resultdata.AddSuccQuantity()
					datacheck := &reporter.ObjChecker{data.value, reply}
					reporter.Datasummer.AddChecker(datacheck)
				}
			}
		}
	}
}
func (self *HashGenerator) hexists(conn redis.Conn, reqnum int32, mytime *time.Timer) {
	length := int32(len(self.data_list))

	var i, j int32 = 0, 0
	for ; j < reqnum; i++ {
		values := self.data_list[i%length]
		for _, data := range values.names {
			if j > reqnum {
				break
			}
			j++

			select {
			case <-mytime.C:
				return
			default:
				reporter.Resultdata.AddSendQuantity()
				_, err := redis.Int(conn.Do("hexists", values.key, data.name))
				if err != nil {
					log.Error("redis hexists  failed: [%s:%s],err %v", values.key, data.name, err)
					reporter.Resultdata.AddFailQuantity()

				} else {
					reporter.Resultdata.AddSuccQuantity()
					//datacheck := &reporter.ObjChecker{1, reply}
					//reporter.Datasummer.AddChecker(datacheck)
				}
			}
		}
	}
}

func (self *HashGenerator) hlen(conn redis.Conn, reqnum int32, mytime *time.Timer) {
	length := int32(len(self.data_list))

	var i int32 = 0
	for ; i < reqnum; i++ {
		select {
		case <-mytime.C:
			return
		default:
			values := self.data_list[i%length]
			reporter.Resultdata.AddSendQuantity()
			_, err := redis.Int(conn.Do("hlen", values.key))
			if err != nil {
				log.Error("redis hlen   failed: [%s],err %v", values.key, err)
				reporter.Resultdata.AddFailQuantity()

			} else {
				reporter.Resultdata.AddSuccQuantity()
			}
		}
	}
}

func (self *HashGenerator) hmget(conn redis.Conn, reqnum int32, mytime *time.Timer) {
	length := int32(len(self.data_list))

	var i int32 = 0
	for ; i < reqnum; i++ {
		select {
		case <-mytime.C:
			return
		default:
			values := self.data_list[i%length]
			key := values.key
			lengh := len(values.names)
			name_slice := make([]interface{}, lengh)
			value_slice := make([]int64, lengh)
			for pos, data := range values.names {
				name_slice[pos] = data.name
				value_slice[pos] = data.value
			}
			reporter.Resultdata.AddSendQuantity()
			reply, err := redis.Values(conn.Do("hmget", key, name_slice))
			if err != nil {
				log.Error("redis hmget   failed: [%s:%v],err %v", key, name_slice, err)
				reporter.Resultdata.AddFailQuantity()

			} else {
				//reporter.Resultdata.AddSuccQuantity()
				datacheck := &reporter.IntArrayChecker{value_slice, reply}
				reporter.Datasummer.AddChecker(datacheck)
			}
		}
	}
}

func (self *HashGenerator) hkeys(conn redis.Conn, reqnum int32, mytime *time.Timer) {
	length := int32(len(self.data_list))

	var i int32 = 0
	for ; i < reqnum; i++ {

		select {
		case <-mytime.C:
			return
		default:
			values := self.data_list[i%length]
			key := values.key
			length := len(values.names)
			name_slice := make([]string, length)
			for pos, data := range values.names {
				name_slice[pos] = data.name
			}
			reporter.Resultdata.AddSendQuantity()
			reply, err := redis.Strings(conn.Do("hkeys", key))
			if err != nil {
				log.Error("redis lpush   failed: [%s],err %v", key, err)
				reporter.Resultdata.AddFailQuantity()

			} else {
				//reporter.Resultdata.AddSuccQuantity()
				datacheck := &reporter.StringArrayChecker{name_slice, reply}
				reporter.Datasummer.AddChecker(datacheck)
			}
		}
	}
}

func (self *HashGenerator) hgetall(conn redis.Conn, reqnum int32, mytime *time.Timer) {

}

func (self *HashGenerator) hdel(conn redis.Conn, reqnum int32, mytime *time.Timer) {
	length := int32(len(self.data_list))

	var i, j int32 = 0, 0
	for ; j < reqnum; i++ {
		values := self.data_list[i%length]
		for _, data := range values.names {
			if j > reqnum {
				break
			}
			j++

			select {
			case <-mytime.C:
				return
			default:
				reporter.Resultdata.AddSendQuantity()
				_, err := redis.Int(conn.Do("hdel", values.key, data.name))
				if err != nil {
					log.Error("redis hdel  failed: [%s:%s],err %v", values.key, data.name, err)
					reporter.Resultdata.AddFailQuantity()

				} else {
					reporter.Resultdata.AddSuccQuantity()
					//datacheck := &reporter.ObjChecker{1, reply}
					//reporter.Datasummer.AddChecker(datacheck)
				}
			}
		}
	}
}

func (self *HashGenerator) del(conn redis.Conn, reqnum int32, mytime *time.Timer) {
	var pos int32 = 0
	for _, values := range self.data_list {
		if pos >= reqnum {
			break
		}
		reporter.Resultdata.AddSendQuantity()
		reply, err := redis.Int(conn.Do("del", values.key))
		//log.Info("redis operating:  del  %s", values.key)
		if err != nil {
			//log.Error("redis %s   failed: %v, %v", cmd, values, err)
			reporter.Resultdata.AddFailQuantity()

		} else {
			if reply >= 0 {
				reporter.Resultdata.AddSuccQuantity()
			} else {
				reporter.Resultdata.AddErrorQuantity()
			}
		}
		pos++
	}
}
