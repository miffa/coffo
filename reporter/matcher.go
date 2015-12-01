package reporter

import (
	"errors"
	"reflect"
	"strconv"
	"time"

	log "code.google.com/p/log4go"
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

/////////////////////////////////////////////////////
//const (
//	CHECK_BEGIN = 0
//	CHECK_END   = 1
//	CHECK_ING   = 99
//)
//
//type UDData struct {
//	Ctr_cmd int
//	SData   string
//	DData   string
//
//	KInt64 int64
//	SInt64 []int64
//	DInt64 []interface{}
//
//	KStr string
//	SStr []string
//	DStr []string
//}
//
//type DataCHecker struct {
//	datachan chan *UDData
//	//ctrlchan  chan int
//	succ_ret  int
//	err_ret   int
//	total_ret int
//	cmd       string
//	datatype  string
//	ok        chan bool
//}
//
//var Datachecker *DataCHecker = &DataCHecker{}
//
//func (self *DataCHecker) Run() {
//	self.datachan = make(chan *UDData, 500000)
//	self.ClearInfo()
//	self.ok = make(chan bool)
//	go self.RunCheck()
//}
//
//func (self *DataCHecker) RunCheck() {
//	for cmd := range self.datachan {
//		//log.Info("%v", cmd)
//		switch cmd.Ctr_cmd {
//		case CHECK_BEGIN:
//			self.datatype = cmd.SData
//			self.cmd = cmd.DData
//		case CHECK_END:
//			self.ok <- true
//		default:
//			self.total_ret++
//			if self.datatype == "string" {
//				if ok := self.CheckString(cmd.SStr, cmd.DStr); ok {
//					self.succ_ret++
//				} else {
//					self.err_ret++
//				}
//			} else if self.datatype == "sortedset" {
//				if ok := self.CheckInt64(cmd.KInt64, cmd.SInt64, cmd.DInt64); ok {
//					self.succ_ret++
//				} else {
//					self.err_ret++
//				}
//			} else if self.datatype == "list" {
//				if ok := self.CheckInt64(cmd.KInt64, cmd.SInt64, cmd.DInt64); ok {
//					self.succ_ret++
//				} else {
//					self.err_ret++
//				}
//			}
//		}
//
//	}
//}
//
//func (self *DataCHecker) Wait() {
//	<-self.ok
//}
//func (self *DataCHecker) ClearInfo() {
//	self.total_ret = 0
//	self.succ_ret = 0
//	self.err_ret = 0
//	self.cmd = ""
//	self.datatype = ""
//}
//
//func (self *DataCHecker) PrintLog() {
//	log.Info("---------------------------------------------------")
//	log.Info("|$ %s:%s       ", self.datatype, self.cmd)
//	log.Info("|$ total check num %d  ", self.total_ret)
//	log.Info("|$ succ  check num %d  ", self.succ_ret)
//	log.Info("|$ err   check num %d  ", self.err_ret)
//	log.Info("---------------------------------------------------\n\n")
//}
//
//func (self *DataCHecker) CheckString(src []string, des []string) bool {
//	/*if len(src) != len(des) {
//		return false
//	}*/
//	for _, data := range des {
//		if data == "" {
//			continue
//		}
//		if ok := self.findinlist(data, src); !ok {
//			log.Info("%s  not in %v", data, src)
//			return false
//		}
//	}
//	return true
//}
//
//func (self *DataCHecker) CheckInt64(key int64, src []int64, des []interface{}) bool {
//
//	for _, data := range des {
//		idata, err := self.Int64(data)
//		if err != nil {
//			log.Error(" int64 error: %v", data)
//			return false
//		}
//		if ok := self.findnuminlist(idata, src); !ok {
//			log.Error(" err result: %d not in %d's set %v", idata, key, src)
//			return false
//		}
//	}
//	return true
//}
//func (self *DataCHecker) Int64(reply interface{}) (int64, error) {
//	switch reply := reply.(type) {
//	case int64:
//		return reply, nil
//	case []byte:
//		n, err := strconv.ParseInt(string(reply), 10, 64)
//		return n, err
//	case nil:
//		return 0, errors.New("nil")
//	}
//	return 0, errors.New("what type")
//}
//func (self *DataCHecker) Contain(obj interface{}, target interface{}) (bool, error) {
//	targetValue := reflect.ValueOf(target)
//	switch reflect.TypeOf(target).Kind() {
//	case reflect.Slice, reflect.Array:
//		for i := 0; i < targetValue.Len(); i++ {
//			if targetValue.Index(i).Interface() == obj {
//				return true, nil
//			}
//		}
//	case reflect.Map:
//		if targetValue.MapIndex(reflect.ValueOf(obj)).IsValid() {
//			return true, nil
//		}
//	}
//
//	return false, errors.New("not in array")
//}
//
//func (self *DataCHecker) AddChecker(data *UDData) {
//	select {
//
//	case self.datachan <- data:
//
//	case <-time.After(time.Second * 2):
//		log.Error("write channel timeout")
//	}
//}
//
//func (self *DataCHecker) findinlist(data string, iinlist []string) bool {
//	for _, temp := range iinlist {
//		if temp == data {
//			return true
//		}
//	}
//	return false
//}
//func (self *DataCHecker) findnuminlist(data int64, iinlist []int64) bool {
//	for _, temp := range iinlist {
//		if temp == data {
//			return true
//		}
//	}
//	return false
//}
//
//func (self *DataCHecker) PrintResultLog() {
//	costime := (Resultdata.GetEndTime() - Resultdata.GetBeginTime()) / 1000 / 1000 / 1000
//	if costime == 0 {
//		costime = 1
//	}
//
//	log.Info("---------------------------------------------------")
//	log.Info("|* %s:%s       ", Resultdata.Data_type, Resultdata.Cmd_type)
//	RequestCount := Resultdata.GetSendQuantity()
//	log.Info("|* total: %d,      Cost_sec: %d, Qps: %d", RequestCount, costime, int64(RequestCount)/costime)
//	RequestSuccCount := Resultdata.GetSuccQuantity()
//	log.Info("|* succ : %d,      Cost_sec: %d, Qps: %d", RequestSuccCount, costime, int64(RequestSuccCount)/costime)
//	RequestFailCount := Resultdata.GetFailQuantity()
//	log.Info("|* fail : %d,  Cost_sec: %d, Qps: %d", RequestFailCount, costime, int64(RequestFailCount)/costime)
//	RequestErrorCount := Resultdata.GetErrorQuantity()
//	if self.total_ret != 0 {
//		log.Info("|* error: %d, Cost_sec: %d, Qps: %d", RequestErrorCount, costime, int64(RequestErrorCount)/costime)
//		log.Info("|*  data check %s:%s       ", self.datatype, self.cmd)
//		log.Info("|*  total check num %d  ", self.total_ret)
//		log.Info("|*  succ  check num %d  ", self.succ_ret)
//		log.Info("|*  err   check num %d  ", self.err_ret)
//	}
//
//	//func (self *DayReport) AddData(data, cmd string, total, succ, fail, err int32) {
//	Dayreport.AddData(self.datatype, self.cmd, RequestCount, RequestCount-int32(self.err_ret), RequestFailCount, int32(self.err_ret))
//
//}
