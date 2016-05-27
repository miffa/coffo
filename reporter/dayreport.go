package reporter

import (
	//"bytes"
	"time"

	log "github.com/alecthomas/log4go"
)

type DayDataItem struct {
	name       string //data+cmd
	msg_send   int32
	msg_failed int32 //send error
	msg_error  int32 //result error
	msg_succ   int32
}

func (self *DayDataItem) Log() {
	log.Info("|$ %s", self.name)
	log.Info("|$ total  : %d", self.msg_send)
	log.Info("|$ succ   : %d", self.msg_succ)
	log.Info("|$ fail   : %d", self.msg_failed)
	log.Info("|$ error  : %d", self.msg_error)
}

func (self *DayDataItem) Clear() {
	self.msg_send = 0
	self.msg_succ = 0
	self.msg_failed = 0
	self.msg_error = 0
}

type DayReport struct {
	datalist map[string]*DayDataItem
	daytimer *time.Timer
}

//func NewDayReporter() *DayReport {
//	return &DayReport{}
//}
var Dayreport *DayReport = &DayReport{}

const (
	DayReportInterval = 12
)

func (self *DayReport) Init() {
	self.datalist = make(map[string]*DayDataItem)

	now := time.Now()
	// 计算下一个零点
	next := now.Add(time.Hour * DayReportInterval)
	next = time.Date(next.Year(), next.Month(), next.Day(), 0, 0, 0, 0, next.Location())
	self.daytimer = time.NewTimer(next.Sub(now))

}

func (self *DayReport) AddData(data, cmd string, total, succ, fail, err int32) {
	name := data + "_" + cmd
	/*	buf := bytes.Buffer{}
		buf.WriteString(data)
		buf.WriteString("_")
		buf.WriteString(cmd)
		name := buf.String()*/
	if value, ok := self.datalist[name]; ok {
		value.msg_error += err
		value.msg_failed += fail
		value.msg_send += total
		value.msg_succ += succ
	} else {
		dataitem := &DayDataItem{name: name, msg_error: err, msg_send: total, msg_failed: fail, msg_succ: succ}
		self.datalist[name] = dataitem
	}
}

func (self *DayReport) PrintLog() {
	for _, v := range self.datalist {
		log.Info("----------daily report--------------")
		v.Log()
		v.Clear()
		log.Info("----------over--------------")
	}
}

func (self *DayReport) Clear() {

}

func (self *DayReport) Run() {
	go func() {
		for {
			select {
			case <-self.daytimer.C:
				self.PrintLog()
				self.Clear()
				self.daytimer.Reset(DayReportInterval * time.Hour)
			}
		}
	}()
}
