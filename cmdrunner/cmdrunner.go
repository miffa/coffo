package cmdrunner

import (
	mygen "coffo/cmddatachecker"
	"coffo/reporter"
	"sync"
	"time"

	log "github.com/alecthomas/log4go"
	"github.com/garyburd/redigo/redis"
)

const (
	RedisMaxIdle   = 4000
	RedisMaxActive = 4000
	RedisTimeout   = 60

	OVERDURE_TIMER    = 1
	UN_OVERDURE_TIMER = 0
)

type CmdRunner struct {
	con_connect_num uint32
	req_num         int32
	ipaddr          string
	//port            int16
	data_struct string
	period      int
	//overdure_time_flag int
	overdure_time_er *time.Timer
	auth_passwd      string
}

func NewCmdRunner(c uint32, req int32, ipandport string /*port int16, */, sec int32, datatype string, t int, authpasswd string) *CmdRunner {
	return &CmdRunner{
		con_connect_num: c,
		req_num:         req,
		ipaddr:          ipandport, //12.34.56.78:80
		//port:            port,
		data_struct: datatype,
		period:      t,
		auth_passwd: authpasswd,
	}
}

func (self *CmdRunner) Run() bool {

	client := &redis.Pool{
		MaxIdle:     RedisMaxIdle,
		MaxActive:   RedisMaxActive,
		IdleTimeout: time.Duration(RedisTimeout * time.Second),
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", self.ipaddr)
			if err != nil {
				log.Error("redis.Dial(\"tcp\", \"%s\") error(%v)", self.ipaddr, err)
				return nil, err
			}
			return c, err
		},
	}

	// cmd path
	cmdlist, ok := mygen.Cmd_map[self.data_struct]
	if !ok {
		return false
	}

	var testdata int = mygen.MAX_TESTDATA
	/*if mygen.MAX_TESTDATA > self.req_num {
		testdata = int(self.req_num)
	}*/
	job_list := make([]mygen.CMDDataChecker, 0)
	var i uint32 = 0
	for ; i < self.con_connect_num; i++ {
		onegen := mygen.NewDataChecker(self.data_struct, client, int(i))
		onegen.Init(testdata) //初始化测试数据
		job_list = append(job_list, onegen)
	}

	reporter.Datasummer.Run()
	reporter.Resultdata.Data_type = self.data_struct

	overdure_time_er := time.NewTimer(time.Duration(int64(self.period)) * time.Second)
	//overdure_time_flag = UN_OVERDURE_TIMER

	for _, cmd := range cmdlist {
		reporter.Resultdata.Cmd_type = cmd
		reporter.Resultdata.SetBeginTime(time.Now().UnixNano())

		wg := new(sync.WaitGroup)
		startcheck := &reporter.CheckBeginer{Datastruct: self.data_struct, Cmd: cmd}

		reporter.Datasummer.AddChecker(startcheck)
		for _, jobitem := range job_list {
			wg.Add(1)
			//job get func
			go jobitem.Check(wg, cmd, self.req_num, overdure_time_er, self.auth_passwd)
		}
		wg.Wait()
		//overdure_time_er.Stop()
		overdure_time_er.Reset(time.Duration(int64(self.period)))
		reporter.Resultdata.SetEndTime(time.Now().UnixNano())
		/*type CheckEnder struct {
		}*/
		endcheck := &reporter.CheckEnder{}
		reporter.Datasummer.AddChecker(endcheck)
		reporter.Datasummer.Wait()
		reporter.Datasummer.PrintResultLog()
		reporter.Datasummer.ClearInfo()
		reporter.Resultdata.Clear()
	}
	log.Info("   ---testing is over---")
	overdure_time_er.Stop()

	//wait for the writing log is over
	time.Sleep(1 * time.Second)
	return true
}

func (self *CmdRunner) RunLoop() bool {

	client := &redis.Pool{
		MaxIdle:     RedisMaxIdle,
		MaxActive:   RedisMaxActive,
		IdleTimeout: RedisTimeout * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", self.ipaddr)
			if err != nil {
				log.Error("redis.Dial(\"tcp\", \"%s\") error(%v)", self.ipaddr, err)
				return nil, err
			}
			return c, err
		},
	}

	// cmd path
	datast := []string{"string", "sortedset", "hash", "set", "list"}
	emgtn := len(datast)
	var pos int = 0
	overdure_time_er := time.NewTimer(time.Duration(int64(self.period)) * time.Second)
	for {
		data_struct := datast[pos%emgtn]
		pos++
		cmdlist, ok := mygen.Cmd_map[data_struct]
		if !ok {
			return false
		}

		var testdata int = mygen.MAX_TESTDATA
		/*if mygen.MAX_TESTDATA > self.req_num {
			testdata = int(self.req_num)
		}*/
		job_list := make([]mygen.CMDDataChecker, 0)
		var i uint32 = 0
		for ; i < self.con_connect_num; i++ {
			onegen := mygen.NewDataChecker(data_struct, client, int(i))

			onegen.Init(testdata) //初始化测试数据
			job_list = append(job_list, onegen)
		}

		reporter.Datasummer.Run()
		reporter.Resultdata.Data_type = data_struct

		for _, cmd := range cmdlist {
			reporter.Resultdata.Cmd_type = cmd
			reporter.Resultdata.SetBeginTime(time.Now().UnixNano())

			wg := new(sync.WaitGroup)
			/*
				type CheckBeginer struct {
					datastruct string
					cmd        string
				}
			*/
			startcheck := &reporter.CheckBeginer{Datastruct: data_struct, Cmd: cmd}

			reporter.Datasummer.AddChecker(startcheck)
			overdure_time_er := time.NewTimer(time.Duration(int64(self.period)) * time.Second)
			overdure_time_er.Reset(time.Duration(int64(self.period)) * time.Second)
			for _, jobitem := range job_list {
				wg.Add(1)
				//job get func
				go jobitem.Check(wg, cmd, self.req_num, overdure_time_er, self.auth_passwd)
			}
			wg.Wait()
			//overdure_time_er.Stop()
			overdure_time_er.Reset(time.Duration(int64(self.period)) * time.Second)
			reporter.Resultdata.SetEndTime(time.Now().UnixNano())
			/*type CheckEnder struct {
			}*/
			endcheck := &reporter.CheckEnder{}
			reporter.Datasummer.AddChecker(endcheck)
			reporter.Datasummer.Wait()
			reporter.Datasummer.PrintResultLog()
			reporter.Datasummer.ClearInfo()
			reporter.Resultdata.Clear()
		}
		log.Info("   ---testing is over---")

	}

	overdure_time_er.Stop()

	//wait for the writing log is over
	time.Sleep(1 * time.Second)
	return true
}
