package reporter

import (
	"errors"
	"reflect"
	"strconv"
	"time"

	log "github.com/alecthomas/log4go"
)

/////////////data check interface////////////////////////
const (
	DATACHECK_OK   = 0
	DATACHECK_ERR  = 1
	DATACHECK_CTRL = 99
)

type DataChecker interface {
	CheckData() int
}

type DataSummer struct {
	begintime time.Time
	endtime   time.Time
	datachan  chan DataChecker
	//ctrlchan  chan int
	succ_ret  int
	err_ret   int
	total_ret int
	fail_ret  int
	Cmd       string
	Datatype  string
	Ok        chan bool
}

var Datasummer *DataSummer = &DataSummer{}

func (self *DataSummer) Run() {
	self.datachan = make(chan DataChecker, 500000)
	self.ClearInfo()
	self.Ok = make(chan bool)
	go self.RunCheck()
}

func (self *DataSummer) RunCheck() {
	for cmd := range self.datachan {
		switch cmd.CheckData() {
		//case DATACHECK_CTRL:
		case DATACHECK_OK:
			self.succ_ret++
			self.total_ret++
		case DATACHECK_ERR:
			self.err_ret++
			self.total_ret++
		}
	}
}

func (self *DataSummer) Wait() {
	<-self.Ok
}
func (self *DataSummer) ClearInfo() {
	self.total_ret = 0
	self.succ_ret = 0
	self.err_ret = 0
	self.Cmd = ""
	self.Datatype = ""
}

func (self *DataSummer) PrintLog() {
	log.Info("---------------------------------------------------")
	log.Info("|$ %s:%s       ", self.Datatype, self.Cmd)
	log.Info("|$ total check num %d  ", self.total_ret)
	log.Info("|$ succ  check num %d  ", self.succ_ret)
	log.Info("|$ err   check num %d  ", self.err_ret)
	log.Info("|$ fail  check num %d  ", self.fail_ret)
	log.Info("---------------------------------------------------\n\n")
}

func (self *DataSummer) AddChecker(data DataChecker) {
	select {

	case self.datachan <- data:

	case <-time.After(time.Second * 2):
		log.Error("write channel timeout")
	}
}

func (self *DataSummer) PrintResultLog() {

	costime := (Resultdata.GetEndTime() - Resultdata.GetBeginTime()) / 1000 / 1000 / 1000
	if costime == 0 {
		costime = 1
	}

	log.Info("---------------------------------------------------")
	log.Info("|* %s:%s       ", Resultdata.Data_type, Resultdata.Cmd_type)
	RequestCount := Resultdata.GetSendQuantity()
	log.Info("|* total: %d,      Cost_sec: %d, Qps: %d", RequestCount, costime, int64(RequestCount)/costime)
	RequestSuccCount := Resultdata.GetSuccQuantity()
	log.Info("|* succ : %d,      Cost_sec: %d, Qps: %d", RequestSuccCount, costime, int64(RequestSuccCount)/costime)
	RequestFailCount := Resultdata.GetFailQuantity()
	log.Info("|* fail : %d,  Cost_sec: %d, Qps: %d", RequestFailCount, costime, int64(RequestFailCount)/costime)
	RequestErrorCount := Resultdata.GetErrorQuantity()
	log.Info("|* error: %d, Cost_sec: %d, Qps: %d", RequestErrorCount, costime, int64(RequestErrorCount)/costime)

	if self.total_ret != 0 {
		log.Info("|*  data check %s:%s       ", self.Datatype, self.Cmd)
		log.Info("|*  total check num %d  ", self.total_ret)
		log.Info("|*  succ  check num %d  ", self.succ_ret)
		log.Info("|*  err   check num %d  ", self.err_ret)
	}

	//func (self *DayReport) AddData(data, cmd string, total, succ, fail, err int32) {
	Dayreport.AddData(self.Datatype, self.Cmd, RequestCount, RequestCount-int32(self.err_ret), RequestFailCount, int32(self.err_ret))
}

/////////////////////// datasumer ctrl ///////////////////
type CheckBeginer struct {
	Datastruct string
	Cmd        string
}

func (self *CheckBeginer) CheckData() int {
	Datasummer.Datatype = self.Datastruct
	Datasummer.Cmd = self.Cmd
	return DATACHECK_CTRL
}

type CheckEnder struct {
}

func (self *CheckEnder) CheckData() int {
	Datasummer.Ok <- true
	return DATACHECK_CTRL
}

//////////////////sortedset check////////////////////////////

type IntArrayChecker struct {
	Myuid   []int64
	Youruid []interface{}
}

func (self *IntArrayChecker) CheckData() int {
	for _, data := range self.Youruid {
		if data == nil {
			continue
		}
		idata, err := self.Int64(data)
		if err != nil {
			log.Error(" int64 error: %v", data)
			return DATACHECK_ERR
		}
		if ok := self.findnuminlist(idata, self.Myuid); !ok {
			log.Error(" err result: %d not in set %v", data, self.Myuid)
			return DATACHECK_ERR
		}
	}
	return DATACHECK_OK
}

func (self *IntArrayChecker) findnuminlist(data int64, iinlist []int64) bool {
	for _, temp := range iinlist {
		if temp == data {
			return true
		}
	}
	return false
}

func (self *IntArrayChecker) Int64(reply interface{}) (int64, error) {
	switch reply := reply.(type) {
	case int64:
		return reply, nil
	case []byte:
		n, err := strconv.ParseInt(string(reply), 10, 64)
		return n, err
	case nil:
		return 0, errors.New("nil")
	}
	return 0, errors.New("what type")
}

///////////////////stringset check///////////////////////
type StringArrayChecker struct {
	Myvalues   []string
	Yourvalues []string
}

func (self *StringArrayChecker) CheckData() int {
	for _, data := range self.Yourvalues {
		if data == "" {
			continue
		}
		if ok := self.findinlist(data, self.Myvalues); !ok {
			log.Info("%s  not in %v", data, self.Myvalues)
			return DATACHECK_ERR
		}
	}
	return DATACHECK_OK
}

func (self *StringArrayChecker) findinlist(data string, iinlist []string) bool {
	for _, temp := range iinlist {
		if temp == data {
			return true
		}
	}
	return false
}

////////////////key in list ///////////////
type StringInArrayChecker struct {
	Aimstr  string
	Aimlist []string
}

func (self *StringInArrayChecker) CheckData() int {
	for _, temp := range self.Aimlist {
		if temp == self.Aimstr {
			return DATACHECK_OK
		}
	}
	return DATACHECK_ERR
}

/////////////value equal //////////////
type ObjChecker struct {
	Src interface{}
	Des interface{}
}

func (self *ObjChecker) CheckData() int {
	targetValue := reflect.ValueOf(self.Des)
	//hopeValue := reflect.ValueOf(Src)
	if targetValue.Interface() == self.Src {
		return DATACHECK_OK
	}
	return DATACHECK_ERR
}
