package cmddatachecker

import (
	"coffo/reporter"
	"strconv"
	"sync"
	"time"

	log "github.com/alecthomas/log4go"
	"github.com/garyburd/redigo/redis"
)

/////////////////////////////////////////////////////////
//           list
/////////////////////////////////////////////////////////

type ListData struct {
	key   string
	queue []string
}

type ListGenerator struct {
	data_list []*ListData
	Client    *redis.Pool
	id        int
}

func (self *ListGenerator) Init(count int) bool {
	//start := time.Now().UnixNano() //debug

	list_value := []string{"cmcmcmcm1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890",
		"cmCMcmCM1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890",
		"CMcmCMcm1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890",
		"CMCMCMCM1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890",
		"cmCMCMcm1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890",
		"CMcmcmCM1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"}
	var length int32 = int32(len(list_value))

	var testquantity int32 = 0
	self.data_list = make([]*ListData, count)
	listcount := int32(count)
	for ; testquantity < listcount; testquantity++ {
		key := MyDataGen.RandomStringKey(STRING_KEY_LEN)
		index := MyIndex.GetUid()
		key = key + strconv.FormatInt(index, 10)
		setlist := make([]string, QUEUE_NUM)
		onesetdata := &ListData{key, setlist}
		for i := 0; i < QUEUE_NUM; i++ {
			setlist[i] = list_value[testquantity%length]
		}
		self.data_list[testquantity%listcount] = onesetdata
	}
	//end := time.Now().UnixNano()
	//输出执行时间，单位为毫秒。
	//log.Info("ListGenerator init use time %d ms", (end-start)/1000000)
	//log.Info("here has [%d] args in it, MAX_TESTDATA[%d]\n", len(self.data_list), MAX_TESTDATA)
	return true
}

func (self *ListGenerator) Check(wg *sync.WaitGroup, cmd string, reqnum int32, mytime *time.Timer, authpasswd string) {

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
	case "lpush": //key value
		self.checkLpush(conn, reqnum, mytime)
	//case "lrem":
	//
	case "llen": //key
		self.checkLlen(conn, reqnum, mytime)
	case "lpop": //key
		self.checkLpop(conn, reqnum, mytime)
	case "rpop": //key
		self.checkRpop(conn, reqnum, mytime)
	case "del":
		self.runDel(conn, reqnum, mytime)
	case "lrange": //key   start stop
		self.checkLrange(conn, reqnum, mytime, 50)
	case "lrange100": //key   start stop[]int{0}
		self.checkLrange(conn, reqnum, mytime, 100)
	case "ltrim": //key   start stop
		self.checkLtrim(conn, reqnum, mytime)
	}
}

func (self *ListGenerator) checkLpush(conn redis.Conn, reqnum int32, mytime *time.Timer) {
	length := int32(len(self.data_list))

	var i int32 = 0
	for ; i < reqnum; i++ {
		values := self.data_list[i%length]
		key := values.key
		for pos := 0; pos < QUEUE_NUM; pos++ {
			select {
			case <-mytime.C:
				return
			default:
				data := values.queue[pos]
				//log.Info("redis lpush   ops: [%d:%d:%d]", key, data)
				reporter.Resultdata.AddSendQuantity()
				_, err := redis.Int(conn.Do("lpush", key, data))
				if err != nil {
					log.Error("redis lpush   failed: [%s:%s],err %v", key, data, err)
					reporter.Resultdata.AddFailQuantity()

				} else {
					reporter.Resultdata.AddSuccQuantity()
					//log.Error("redis  lpush  failed: %v", values)
				}
			}
		}
	}
}

func (self *ListGenerator) checkLlen(conn redis.Conn, reqnum int32, mytime *time.Timer) {
	length := int32(len(self.data_list))

	var i int32 = 0
	for ; i < reqnum; i++ {
		select {
		case <-mytime.C:
			return
		default:
			values := self.data_list[i%length]
			//log.Info("redis llen   ops: [%d:%d:%d]", key)
			reporter.Resultdata.AddSendQuantity()
			_, err := redis.Int(conn.Do("llen", values.key))
			if err != nil {
				log.Error("redis llen   failed: [%s],err %v", values.key, err)
				reporter.Resultdata.AddFailQuantity()

			} else {
				reporter.Resultdata.AddSuccQuantity()
				//log.Error("redis  set  failed: %v", values)
			}
		}

	}
}

func (self *ListGenerator) checkLpop(conn redis.Conn, reqnum int32, mytime *time.Timer) {
	length := int32(len(self.data_list))

	var i int32 = 0
	for ; i < reqnum; i++ {
		select {
		case <-mytime.C:
			return
		default:
			values := self.data_list[i%length]
			//log.Info("redis lpop   ops: [%d:%d:%d]", key)
			reporter.Resultdata.AddSendQuantity()
			reply, err := conn.Do("lpop", values.key)
			if err != nil {
				log.Error("redis lpop   failed: [%s],err %v", values.key, err)
				reporter.Resultdata.AddFailQuantity()

			} else if reply == nil {
				reporter.Resultdata.AddSuccQuantity()

			} else {
				//reporter.Resultdata.AddSuccQuantity()
				/*type StringInArrayChecker struct {
					aimstr  string
					aimlist []string
				}*/
				value, _ := redis.String(reply, nil)
				datacheck := &reporter.StringInArrayChecker{Aimstr: value, Aimlist: values.queue}
				reporter.Datasummer.AddChecker(datacheck)
			}
		}

	}
}

func (self *ListGenerator) checkRpop(conn redis.Conn, reqnum int32, mytime *time.Timer) {
	length := int32(len(self.data_list))

	var i int32 = 0
	for ; i < reqnum; i++ {
		select {
		case <-mytime.C:
			return
		default:
			values := self.data_list[i%length]
			//log.Info("redis rpop   ops: [%d:%d:%d]", key)
			reporter.Resultdata.AddSendQuantity()
			reply, err := conn.Do("rpop", values.key)
			if err != nil {
				log.Error("redis rpop   failed: [%s],err %v", values.key, err)
				reporter.Resultdata.AddFailQuantity()

			} else if reply == nil {
				reporter.Resultdata.AddSuccQuantity()

			} else {

				//reporter.Resultdata.AddSuccQuantity()
				/*type StringInArrayChecker struct {
					aimstr  string
					aimlist []string
				}*/
				value, _ := redis.String(reply, nil)
				datacheck := &reporter.StringInArrayChecker{Aimstr: value, Aimlist: values.queue}
				reporter.Datasummer.AddChecker(datacheck)
			}
		}

	}
}

func (self *ListGenerator) checkLrange(conn redis.Conn, reqnum int32, mytime *time.Timer, count int) {
	length := int32(len(self.data_list))

	var i int32 = 0
	for ; i < reqnum; i++ {
		select {
		case <-mytime.C:
			return
		default:
			values := self.data_list[i%length]
			//log.Info("redis lrange   ops: [%d:%d:%d]", key)
			reporter.Resultdata.AddSendQuantity()
			reply, err := redis.Strings(conn.Do("lrange", values.key, 0, count))
			if err != nil {
				log.Error("redis lrange   failed: [%s],err %v", values.key, err)
				reporter.Resultdata.AddFailQuantity()

			} else {
				//reporter.Resultdata.AddSuccQuantity()
				datacheck := &reporter.StringArrayChecker{Myvalues: values.queue, Yourvalues: reply}
				reporter.Datasummer.AddChecker(datacheck)
			}
		}

	}
}

func (self *ListGenerator) checkLtrim(conn redis.Conn, reqnum int32, mytime *time.Timer) {
	length := int32(len(self.data_list))

	var i int32 = 0
	for ; i < reqnum; i++ {
		select {
		case <-mytime.C:
			return
		default:
			values := self.data_list[i%length]
			//log.Info("redis ltrim   ops: [%d:%d:%d]", key)
			reporter.Resultdata.AddSendQuantity()
			_, err := redis.String(conn.Do("ltrim", values.key, 0, 10)) //
			if err != nil {
				log.Error("redis ltrim   failed: [%s],err %v", values.key, err)
				reporter.Resultdata.AddFailQuantity()

			} else {
				reporter.Resultdata.AddSuccQuantity()
			}
		}
	}
}

func (self *ListGenerator) runDel(conn redis.Conn, req_num int32, mytime *time.Timer) {

	var pos int32 = 0
	for _, values := range self.data_list {

		if pos >= req_num {
			break
		}
		reporter.Resultdata.AddSendQuantity()
		reply, err := redis.Int(conn.Do("del", values.key))
		//log.Info("redis operating:  del  %s", values.key)
		if err != nil {
			log.Error("redis %s   failed: %v, %v", "del", values.key, err)
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
