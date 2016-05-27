package cmddatachecker

import (
	"bufio"
	"coffo/reporter"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/alecthomas/log4go"
	"github.com/garyburd/redigo/redis"
)


var Filepath string 
/////////////////////////////////////////////////////////
//           string
/////////////////////////////////////////////////////////
const (
	MGET_GROUP_avg = 20
	MGET_GROUP_32  = 32
	MGET_GROUP_64  = 64
	STRING_KEY_LEN = 128
	STRING_VAL_LEN = 1024
)

type StringData struct {
	key   string
	value string
}
type StringGenerator struct {
	data_list []*StringData
	Client    *redis.Pool
	id        int
}

func (self *StringGenerator) GenData2File(testdata int, filename string) bool {
	// var outputWriter *bufio.Writer
	// var outputFile *os.File
	// var outputError os.Error
	// var outputString string
	outputFile, outputError := os.Open(filename)
	if outputError != nil {
		log.Error("An error occurred with file opening %s :%v", filename, outputError)
		return false
	}
	defer outputFile.Close()

	outputreader := bufio.NewReader(outputFile)

	self.data_list = make([]*StringData, 0)
        mynum := 0
	for {
		dataline, inputerr := outputreader.ReadString('\n')
		if inputerr == io.EOF {
			break
		}
		datas := strings.Split(dataline, " ")
		if len(datas) < 1 {
			continue
		}
		data := &StringData{datas[0], datas[1]}
		self.data_list = append(self.data_list, data)
                mynum ++
                if mynum > testdata +2 {
                    break   
                }
		//fmt.Printf("key:%s, value:%s\n", datas[0], datas[1])
	}
	return true
}

func (self *StringGenerator) SInit(testdata int) bool {
	filenam := fmt.Sprintf("%s/qdb_data_%d.log", Filepath, self.id)
	//go GenData2File(testdata, filenam, wg)
	self.GenData2File(testdata, filenam)
	log.Info(" datafile %s is ok \n", filenam)
	fmt.Printf(" datafile %s is ok", filenam)
	return true
}

func (self *StringGenerator) Init(testdata int) bool {
	//init test data
	//start := time.Now().UnixNano() //debug
	self.data_list = make([]*StringData, 0)
	var testquantity int = 0
	for testquantity < testdata {
		index := MyIndex.GetUid()
		key := MyDataGen.RandomStringKey(STRING_KEY_LEN)
		key = key + strconv.FormatInt(index, 10)
		value := key + MyDataGen.RandomStringKey(STRING_VAL_LEN)
		data := &StringData{key, value}
		self.data_list = append(self.data_list, data)
		testquantity++
	}

	//end := time.Now().UnixNano()
	//输出执行时间，单位为毫秒。
	//log.Info("StringGenerator init use time %d ms", (end-start)/1000000)
	//log.Info("here has [%d] args in it, MAX_TESTDATA[%d]\n", len(self.data_list), MAX_TESTDATA)
	return true
}

func (self *StringGenerator) Check(wg *sync.WaitGroup, cmd string, reqnum int32, mytime *time.Timer, authpasswd string) {
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
	case "set":
		self.checkSet(conn, reqnum, mytime)
	case "get":
		self.checkGet(conn, reqnum, mytime)
	case "del":
		self.runDel(conn, reqnum, mytime)
	case "mget":
		self.checkMGetRunner(conn, reqnum, MGET_GROUP_avg, mytime)
	case "mget32":
		self.checkMGetRunner(conn, reqnum, MGET_GROUP_32, mytime)
	case "mget64":
		self.checkMGetRunner(conn, reqnum, MGET_GROUP_64, mytime)
	}
	return
}

func (self *StringGenerator) checkSet(conn redis.Conn, reqnum int32, mytime *time.Timer) {

	//执行req_num次,重复利用args数组中的数据,直到执行了req_num次结束
	length := int32(len(self.data_list))
	var i int32 = 0
	for ; i < reqnum; i++ {
		select {
		case <-mytime.C:
			return
		default:
			values := self.data_list[i%length]
			//log.Info("redis operating:set   %v", values)
			reporter.Resultdata.AddSendQuantity()
			reply, err := redis.String(conn.Do("set", values.key, values.value))
			if err != nil {
				//log.Error("redis %s   failed: %v, %v", cmd, values, err)
				reporter.Resultdata.AddFailQuantity()

			} else {
				if "OK" == reply {
					reporter.Resultdata.AddSuccQuantity()
				} else {
					reporter.Resultdata.AddErrorQuantity()
					//log.Error("redis  set  failed: %v", values)
				}

			}
		}
	}
}

func (self *StringGenerator) checkGet(conn redis.Conn, reqnum int32, mytime *time.Timer) {

	//执行req_num次,重复利用args数组中的数据,直到执行了req_num次结束
	length := int32(len(self.data_list))
	var i int32 = 0
	for ; i < reqnum; i++ {
		select {
		case <-mytime.C:
			return
		default:
			values := self.data_list[i%length]
			reporter.Resultdata.AddSendQuantity()
			//reply, err := redis.String(conn.Do("get", values.key))
			_, err := conn.Do("get", values.key)
			//log.Info("redis operating:  get  %s", values.key)
			if err != nil {
				//log.Error("redis %s   failed: %v, %v", cmd, values, err)
				reporter.Resultdata.AddFailQuantity()

			} else {
				/*if values.value == reply {
					reporter.Resultdata.AddSuccQuantity()
				} else {
					reporter.Resultdata.AddErrorQuantity()
					//log.Error("redis  set  failed: %v", values)
				}*/
				reporter.Resultdata.AddSuccQuantity()
			}
		}
	}
}

func (self *StringGenerator) runDel(conn redis.Conn, req_num int32, mytime *time.Timer) {

	var pos int32 = 0
	for _, values := range self.data_list {
		if pos >= req_num {
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
	/*
		if pos >= req_num {
						log.Info(" %d > %d, end del", pos, req_num)
								} else {
												log.Info("del all data in data_list")
														}*/
}

func (self *StringGenerator) checkMGetRunner(conn redis.Conn, reqnum int32, group_num int32, mytime *time.Timer) {

	//执行req_num次,重复利用args数组中的数据,直到执行了req_num次结束
	length := int32(len(self.data_list))
	var i int32 = 0
	var arg_pos int32 = 0
	for i = 0; i < reqnum; i++ {
		select {
		case <-mytime.C:
			return
		default:
			var pos int32 = 0
			seg_keys := make([]interface{}, group_num)
			seg_value := make([]string, group_num)
			for ; pos < group_num; pos++ {
				values := self.data_list[arg_pos%length]
				seg_keys[pos] = values.key
				seg_value[pos] = values.value
				arg_pos++
			}
			self.runMget(seg_keys, seg_value, conn)
		}
	}

}

func (self *StringGenerator) runMget(keys []interface{}, values []string, conn redis.Conn) {

	reporter.Resultdata.AddSendQuantity()
	reply, err := redis.Strings(conn.Do("mget", keys...))
	//log.Info("redis operating:  mget  %d:%v", len(keys), keys)
	if err != nil {
		log.Error("redis  mget [%v]   failed: ,err: [%v]", values, err)
		reporter.Resultdata.AddFailQuantity()
	} else {
		/*type StringArrayChecker struct {
		myvalues   []string
					yourvalues []string
							}*/
		datacheck := &reporter.StringArrayChecker{Myvalues: values, Yourvalues: reply}
		reporter.Datasummer.AddChecker(datacheck)
	}
}

func (self *StringGenerator) findinlist(data string, iinlist []string) bool {
	for _, temp := range iinlist {
		if temp == data {
			return true
		}
	}
	return false
}
