package cmddatachecker

import (
	"coffo/reporter"
	"sync"
	"time"

	log "github.com/alecthomas/log4go"
	"github.com/garyburd/redigo/redis"
)

/////////////////////////////////////////////////////////
//           list
/////////////////////////////////////////////////////////

type SetData struct {
	key   int64
	queue []int64
}

type SetGenerator struct {
	data_list []*SetData
	Client    *redis.Pool
	id        int
}

func (self *SetGenerator) Init(count int) bool {
	//start := time.Now().UnixNano() //debug
	var testquantity int32 = 0
	self.data_list = make([]*SetData, count)
	listcount := int32(count)
	pos_list := [9]int{2, 3, 1, 3, 20, 20, 2, 1, 1}
	for ; testquantity < listcount; testquantity++ {
		index := MyIndex.GetUid()
		lenpos := pos_list[testquantity%9]
		setlist := make([]int64, lenpos)
		onesetdata := &SetData{index, setlist}
		for i := 0; i < lenpos; i++ {
			value := MyDataGen.Random64Key(UID_SEED)
			setlist[i] = value
		}
		self.data_list[testquantity%listcount] = onesetdata
	}
	//end := time.Now().UnixNano()
	//输出执行时间，单位为毫秒。
	//log.Info("SetGenerator init use time %d ms", (end-start)/1000000)
	//log.Info("here has [%d] args in it, MAX_TESTDATA[%d]\n", len(self.data_list), MAX_TESTDATA)
	return true
}

func (self *SetGenerator) Check(wg *sync.WaitGroup, cmd string, reqnum int32, mytime *time.Timer, authpasswd string) {

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
	switch cmd {
	case "sadd": //key value
		self.sadd(conn, reqnum, mytime)
	case "sismember": //key v : int 0 1
		self.sismember(conn, reqnum, mytime)
	case "smembers": //key  : []
		self.smembers(conn, reqnum, mytime)
	case "scard": //key : int
		self.scard(conn, reqnum, mytime)
	case "srem": //key v : int 0 1
		self.srem(conn, reqnum, mytime)
	case "spop": //key : v
		self.spop(conn, reqnum, mytime)
	case "del": //key : int 0 1
		self.del(conn, reqnum, mytime)
	}
}

func (self *SetGenerator) sadd(conn redis.Conn, reqnum int32, mytime *time.Timer) {
	length := int32(len(self.data_list))

	var i int32 = 0
	for ; i < reqnum; i++ {
		values := self.data_list[i%length]
		key := values.key
		//for pos := 0; pos < QUEUE_NUM; pos++ {
		for _, data := range values.queue {
			select {
			case <-mytime.C:
				return
			default:
				reporter.Resultdata.AddSendQuantity()
				_, err := redis.Int(conn.Do("sadd", key, data))
				if err != nil {
					log.Error("redis lpush   failed: [%d:%d],err %v", key, data, err)
					reporter.Resultdata.AddFailQuantity()

				} else {
					reporter.Resultdata.AddSuccQuantity()
				}
			}
		}
	}
}

func (self *SetGenerator) sismember(conn redis.Conn, reqnum int32, mytime *time.Timer) {
	length := int32(len(self.data_list))

	var i, j int32 = 0, 0
	for ; j < reqnum; i++ {
		values := self.data_list[i%length]

		for _, data := range values.queue {
			if j > reqnum {
				break
			}
			j++

			select {
			case <-mytime.C:
				return
			default:
				reporter.Resultdata.AddSendQuantity()

				_, err := redis.Int(conn.Do("sismember", values.key, data))
				if err != nil {
					log.Error("redis sismember   failed: [%s:%s],err %v", values.key, data, err)
					reporter.Resultdata.AddFailQuantity()

				} else {
					reporter.Resultdata.AddSuccQuantity()
				}
			}
		}
	}
}

func (self *SetGenerator) smembers(conn redis.Conn, reqnum int32, mytime *time.Timer) {
	length := int32(len(self.data_list))

	var i int32 = 0
	for ; i < reqnum; i++ {
		select {
		case <-mytime.C:
			return
		default:
			values := self.data_list[i%length]

			reporter.Resultdata.AddSendQuantity()
			reply, err := redis.Values(conn.Do("smembers", values.key))
			if err != nil {
				log.Error("redis sismember   failed: [%s],err %v", values.key, err)
				reporter.Resultdata.AddFailQuantity()

			} else {
				datacheck := &reporter.IntArrayChecker{Myuid: values.queue, Youruid: reply}
				reporter.Datasummer.AddChecker(datacheck)
			}
		}
	}
}

func (self *SetGenerator) scard(conn redis.Conn, reqnum int32, mytime *time.Timer) {
	length := int32(len(self.data_list))

	var i int32 = 0
	for ; i < reqnum; i++ {
		select {
		case <-mytime.C:
			return
		default:
			values := self.data_list[i%length]

			reporter.Resultdata.AddSendQuantity()
			//reply, err := redis.Int(conn.Do("scard", values.key))
			_, err := conn.Do("scard", values.key)
			if err != nil {
				log.Error("redis scard   failed: [%s],err %v", values.key, err)
				reporter.Resultdata.AddFailQuantity()
			} else {
				reporter.Resultdata.AddSuccQuantity()
			}
		}
	}
}

func (self *SetGenerator) spop(conn redis.Conn, reqnum int32, mytime *time.Timer) {
	length := int32(len(self.data_list))

	var i int32 = 0
	for ; i < reqnum; i++ {
		select {
		case <-mytime.C:
			return
		default:
			values := self.data_list[i%length]

			reporter.Resultdata.AddSendQuantity()
			reply, err := conn.Do("spop", values.key)
			//_, err := conn.Do("scard", values.key)
			if err != nil {
				log.Error("redis spop  failed: [%s],err %v", values.key, err)
				reporter.Resultdata.AddFailQuantity()
			} else if reply == nil {
				reporter.Resultdata.AddSuccQuantity()

			} else {
				//value, _ := redis.Int(reply, err)
				reporter.Resultdata.AddSuccQuantity()
			}
		}
	}
}

func (self *SetGenerator) srem(conn redis.Conn, reqnum int32, mytime *time.Timer) {
	length := int32(len(self.data_list))

	var i, j int32 = 0, 0
	for j < reqnum {
		values := self.data_list[i%length]
		i++
		for _, data := range values.queue {
			if j > reqnum {
				break
			}
			j++

			select {
			case <-mytime.C:
				return
			default:
				reporter.Resultdata.AddSendQuantity()

				_, err := redis.Int(conn.Do("srem", values.key, data))
				if err != nil {
					log.Error("redis srem   failed: [%s:%s],err %v", values.key, data, err)
					reporter.Resultdata.AddFailQuantity()

				} else {
					reporter.Resultdata.AddSuccQuantity()
				}
			}
		}
	}
}

func (self *SetGenerator) del(conn redis.Conn, reqnum int32, mytime *time.Timer) {
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
			if reply > 0 {
				reporter.Resultdata.AddSuccQuantity()
			} else {
				reporter.Resultdata.AddErrorQuantity()
			}
		}
		pos++
	}
}
