package cmddatachecker

import (
	log "github.com/alecthomas/log4go"
	//"container/list"
	"coffo/reporter"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

const (
	UID_SEED = 100000
	//UID_SEED    = 10000000000000000
	ZCOUNT_NUM  = 2000
	ZRANGE_100  = 100
	ZRANGE_500  = 500
	ZRANGE_1000 = 1000
	ZRANGE_2000 = 2000
	ZRANGE_ALL  = 10
)

type SortedSetData struct {
	key        int64
	sortedlist []int64
}

type SortedSetGenerator struct {
	data_list []*SortedSetData
	Client    *redis.Pool
	id        int
}

func (self *SortedSetGenerator) Init(testdata_num int) bool {
	//start := time.Now().UnixNano() //debug
	var testquantity int = 0
	self.data_list = make([]*SortedSetData, MAX_TESTDATA)
	for testquantity < testdata_num {
		if testquantity < ZRANGE_ALL {
			//var myuid int64 = MyDataGen.Random64Key(UID_SEED)
			var myuid int64 = MyUid.GetUid()
			setlist := make([]int64, ZCOUNT_NUM)
			onesetdata := &SortedSetData{myuid, setlist}
			for i := 0; i < ZCOUNT_NUM; i++ {
				setlist[i] = MyDataGen.Random64Key(UID_SEED)
			}
			self.data_list[testquantity] = onesetdata
		} else {
			//var myuid int64 = MyDataGen.Random64Key(UID_SEED)
			var myuid int64 = MyUid.GetUid()
			setlist5 := make([]int64, 5)
			onesetdata := &SortedSetData{myuid, setlist5}
			for i := 0; i < 5; i++ {
				setlist5[i] = MyDataGen.Random64Key(UID_SEED)
			}
			self.data_list[testquantity] = onesetdata
		}
		testquantity++
	}
	//end := time.Now().UnixNano()
	//输出执行时间，单位为毫秒。
	//log.Info("SortedSetGenerator init use time %d ms", (end-start)/1000000)
	//log.Info("here has [%d] args in it, MAX_TESTDATA[%d]\n", len(self.data_list), MAX_TESTDATA)
	return true
}

func (self *SortedSetGenerator) Check(wg *sync.WaitGroup, cmd string, reqnum int32, mytime *time.Timer, authpasswd string) {

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
	case "zadd":
		self.checkZadd5(conn)
		self.checkZadd(conn, reqnum, mytime)
	case "zrevrange":
		self.checkZrevrange(conn, reqnum, mytime)
		//case "zrevrangebyscore":
		//	fallthrough
	case "zcount":
		self.checkZcount(conn, reqnum, mytime)
	case "zrange":
		self.checkZrange(conn, reqnum, ZRANGE_100, mytime)
	case "zrange500":
		self.checkZrange(conn, reqnum, ZRANGE_500, mytime)
	case "zrange1000":
		self.checkZrange(conn, reqnum, ZRANGE_1000, mytime)
	case "zrange2000":
		self.checkZrange(conn, reqnum, ZRANGE_2000, mytime)
	case "del":
		self.runDel(conn, reqnum)
	case "zrem":
		self.checkZrem(conn, reqnum, mytime)
		//self.checkZremOk(reqnum)
	case "zscore":
		self.checkZscore(conn, reqnum, mytime)
		//self.checkZscoreOk(reqnum)
	}
}

func (self *SortedSetGenerator) checkZadd5(conn redis.Conn) {
	//length := int32(len(self.data_list))
	//优先加载10个默认用户
	var i int32 = 0
	for ; i < ZRANGE_ALL; i++ {
		values := self.data_list[i]
		weight := 0
		for _, data := range values.sortedlist {
			weight++
			//log.Info("redis zadd   ops: [%d:%d:%d]", values.key, weight, data)
			reporter.Resultdata.AddSendQuantity()
			_, err := redis.Int(conn.Do("zadd", values.key, weight, data))
			if err != nil {
				log.Error("redis zadd   failed: [%d:%d],err %v", values.key, data, err)
				reporter.Resultdata.AddFailQuantity()

			} else {
				reporter.Resultdata.AddSuccQuantity()
				//log.Error("redis  set  failed: %v", values)
			}
		}
	}
}

func (self *SortedSetGenerator) checkZadd(conn redis.Conn, reqnum int32, mytime *time.Timer) {

	//执行req_num次,重复利用args数组中的数据,直到执行了req_num次结束
	length := int32(len(self.data_list))
	var i int32 = 0
	var testquantity int32 = 10
	var index int = 0
	for ; i < reqnum; i++ {
		select {
		case <-mytime.C:
			return
		default:
			values := self.data_list[testquantity%length]
			key := values.key
			data := values.sortedlist[index%len(values.sortedlist)]
			//log.Info("redis zadd   ops: [%d:%d:%d]", key, index, data)
			reporter.Resultdata.AddSendQuantity()
			_, err := redis.Int(conn.Do("zadd", key, index, data))
			if err != nil {
				log.Error("redis zadd   failed: [%d:%d],err %v", values.key, data, err)
				reporter.Resultdata.AddFailQuantity()

			} else {
				reporter.Resultdata.AddSuccQuantity()
				//log.Error("redis  set  failed: %v", values)
			}
			testquantity++
			if testquantity%length == 0 {
				index++
			}
		}
	}
}

func (self *SortedSetGenerator) checkZrevrange(conn redis.Conn, req_num int32, mytime *time.Timer) {

	var pos int32 = 0
	for _, value := range self.data_list {
		select {
		case <-mytime.C:
			return
		default:
			if pos >= req_num {
				break
			}
			reporter.Resultdata.AddSendQuantity()
			reply, err := redis.Values(conn.Do("zrevrange", value.key, 0, ZRANGE_100))
			//log.Info("redis operating:  zrevrange %d  0  %d", value.key, ZRANGE_100)
			if err != nil {
				log.Info("redis operating:  zrevrange %d  0  %d  error %v", value.key, ZRANGE_100, err)
				reporter.Resultdata.AddFailQuantity()
			} else {
				if len(reply) > 0 {
					reporter.Resultdata.AddSuccQuantity()
				} else {
					reporter.Resultdata.AddErrorQuantity()
				}
			}
			pos++
		}
	}
}

func (self *SortedSetGenerator) checkZcount(conn redis.Conn, req_num int32, mytime *time.Timer) {

	var pos int32 = 0
	for _, value := range self.data_list {
		select {
		case <-mytime.C:
			return
		default:
			if pos >= req_num {
				break
			}
			reporter.Resultdata.AddSendQuantity()
			reply, err := redis.Int(conn.Do("zcount", value.key, "-inf", "+inf"))
			//log.Info("redis operating:  zcount  %d -inf +inf", value.key)
			if err != nil {
				log.Error("redis operating:  zcount  %d  error %v", value.key, err)
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
}
func (self *SortedSetGenerator) checkZrange(conn redis.Conn, req_num int32, range_data int32, mytime *time.Timer) {

	var pos int32 = 0
	for ; pos < req_num; pos++ {
		select {
		case <-mytime.C:
			return
		default:
			value := self.data_list[pos%ZRANGE_ALL]
			reporter.Resultdata.AddSendQuantity()
			reply, err := redis.Values(conn.Do("ZRANGE", value.key, 0, range_data))
			//log.Info("redis operating:  zrange  %d  0  %d", value.key, range_data)
			if err != nil {
				log.Info("redis operating:  zrange %d  0  %d  error %v", value.key, range_data, err)
				reporter.Resultdata.AddFailQuantity()
			} else {
				//log.Info(" zrange %d 0 %d ret:%v", value.key, range_data, reply)
				/*type IntArrayChecker struct {
				myuid   []int64
									youruid []interface{}
													}*/
				datacheck := &reporter.IntArrayChecker{Myuid: value.sortedlist, Youruid: reply}
				reporter.Datasummer.AddChecker(datacheck)
			}
		}
	}
}

func (self *SortedSetGenerator) checkZremOk(conn redis.Conn, reqnum int32, mytime *time.Timer) {

	length := int32(len(self.data_list))
	var i int32 = 0
	var index int = -1
	for ; i < reqnum; i++ {

		if i%length == 0 {
			index++
		}
		values := self.data_list[i%length]
		key := values.key
		data := values.sortedlist[index%len(values.sortedlist)]
		//log.Info("redis zrem   ops: [%d:%d]", key, data)
		reporter.Resultdata.AddSendQuantity()
		_, err := redis.Int(conn.Do("zrem", key, data))
		if err != nil {
			log.Error("redis zrem   failed: [%d:%d],err %v", values.key, data, err)
			reporter.Resultdata.AddFailQuantity()

		} else {
			reporter.Resultdata.AddSuccQuantity()
		}
		if i%length == 0 {
			index++
		}
	}
}

func (self *SortedSetGenerator) checkZrem(conn redis.Conn, reqnum int32, mytime *time.Timer) {

	//执行req_num次,重复利用args数组中的数据,直到执行了req_num次结束
	length := int32(len(self.data_list))
	var i int32 = 0
	for i < reqnum {
		values := self.data_list[i%length]
		key := values.key
		for _, value := range values.sortedlist {
			select {
			case <-mytime.C:
				return
			default:
				if i > reqnum {
					break
				}
				i++
				//log.Info("redis zrem   ops: [%d:%d]", key, value)
				reporter.Resultdata.AddSendQuantity()
				_, err := conn.Do("zrem", key, value)
				if err != nil {
					log.Error("redis zrem   failed: [%d:%d],err %v", key, value, err)
					reporter.Resultdata.AddFailQuantity()

				} else {
					reporter.Resultdata.AddSuccQuantity()
				}
			}
		}

	}
}

func (self *SortedSetGenerator) checkZscore(conn redis.Conn, reqnum int32, mytime *time.Timer) {

	//执行req_num次,重复利用args数组中的数据,直到执行了req_num次结束
	length := int32(len(self.data_list))
	var i int32 = 0
	for i < reqnum {
		values := self.data_list[i%length]
		key := values.key
		for _, value := range values.sortedlist {
			select {
			case <-mytime.C:
				return
			default:
				if i > reqnum {
					break
				}
				i++
				//log.Info("redis zscore   ops: [%d:%d]", key, value)
				reporter.Resultdata.AddSendQuantity()
				_, err := conn.Do("zscore", key, value)
				if err != nil {
					log.Error("redis zscore   failed: [%d:%d],err %v", key, value, err)
					reporter.Resultdata.AddFailQuantity()

				} else {
					reporter.Resultdata.AddSuccQuantity()
				}
			}
		}

	}
}

func (self *SortedSetGenerator) checkZscoreOk(conn redis.Conn, reqnum int32, mytime *time.Timer) {

	length := int32(len(self.data_list))
	var i int32 = 0
	var index int32 = -1
	for ; i < reqnum; i++ {
		if i%length == 0 {
			index++
		}
		values := self.data_list[i%length]
		key := values.key
		data := values.sortedlist[index]
		//log.Info("redis zscore   ops: [%d:%d]", key, data)
		reporter.Resultdata.AddSendQuantity()
		_, err := redis.Int(conn.Do("zscore", key, data))
		if err != nil {
			log.Error("redis zscore   failed: [%d:%d],err %v", key, data, err)
			reporter.Resultdata.AddFailQuantity()

		} else {
			reporter.Resultdata.AddSuccQuantity()
		}
		if i%length == 0 {
			index++
		}
	}
}
func (self *SortedSetGenerator) runDel(conn redis.Conn, req_num int32) {

	var pos int32 = 0
	for _, value := range self.data_list {
		if pos >= req_num {
			break
		}
		reporter.Resultdata.AddSendQuantity()
		_, err := conn.Do("del", value.key)
		//log.Info("redis operating:  del  %d", value.key)
		if err != nil {
			log.Error("redis operating:  del  %d  error %v", value.key, err)
			reporter.Resultdata.AddFailQuantity()
		} else {
			reporter.Resultdata.AddSuccQuantity()
		}
		pos++
	}
}
