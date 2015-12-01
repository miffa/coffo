package reporter

import (
	log "code.google.com/p/log4go"
	"sync/atomic"
)

type Data struct {
	key   string
	value int64
}

type ResultData struct {
	Data_type  string
	Cmd_type   string
	begin_time int64
	end_time   int64
	msg_send   int32
	msg_failed int32 //send error
	msg_error  int32 //result error
	msg_succ   int32
}

var Resultdata *ResultData = &ResultData{begin_time: 0, end_time: 0, msg_send: 0, msg_failed: 0, msg_error: 0}

func (self *ResultData) SetEndTime(sec int64) {
	atomic.StoreInt64(&self.end_time, sec)
}

func (self *ResultData) GetEndTime() int64 {
	return atomic.LoadInt64(&self.end_time)
}

func (self *ResultData) SetBeginTime(sec int64) {
	atomic.StoreInt64(&self.begin_time, sec)
}

func (self *ResultData) GetBeginTime() int64 {
	return atomic.LoadInt64(&self.begin_time)
}

func (self *ResultData) AddSendQuantity() int32 {
	return atomic.AddInt32(&self.msg_send, 1)
}

func (self *ResultData) GetSendQuantity() int32 {
	return atomic.LoadInt32(&self.msg_send)
}

func (self *ResultData) AddFailQuantity() int32 {
	return atomic.AddInt32(&self.msg_failed, 1)
}

func (self *ResultData) GetFailQuantity() int32 {
	return atomic.LoadInt32(&self.msg_failed)
}

func (self *ResultData) AddErrorQuantity() int32 {
	return atomic.AddInt32(&self.msg_error, 1)
}

func (self *ResultData) GetErrorQuantity() int32 {
	return atomic.LoadInt32(&self.msg_error)
}

func (self *ResultData) AddSuccQuantity() int32 {
	return atomic.AddInt32(&self.msg_succ, 1)
}

func (self *ResultData) GetSuccQuantity() int32 {
	return atomic.LoadInt32(&self.msg_succ)
}

func (self *ResultData) PrintLog() {
	costime := (self.GetEndTime() - self.GetBeginTime()) / 1000 / 1000 / 1000
	if costime == 0 {
		costime = 1
	}

	log.Info("---------------------------------------------------")
	log.Info("|$ %s:%s       ", self.Data_type, self.Cmd_type)
	RequestCount := self.GetSendQuantity()
	log.Info("|$ total: %d,      Cost_sec: %d, Qps: %d", RequestCount, costime, int64(RequestCount)/costime)
	RequestSuccCount := self.GetSuccQuantity()
	log.Info("|$ succ : %d,      Cost_sec: %d, Qps: %d", RequestSuccCount, costime, int64(RequestSuccCount)/costime)
	RequestFailCount := self.GetFailQuantity()
	log.Info("|$ fail : %d,  Cost_sec: %d, Qps: %d", RequestFailCount, costime, int64(RequestFailCount)/costime)
	RequestErrorCount := self.GetErrorQuantity()
	log.Info("|$ error: %d, Cost_sec: %d, Qps: %d", RequestErrorCount, costime, int64(RequestErrorCount)/costime)
	log.Info("---------------------------------------------------\n\n")
}

func (self *ResultData) Clear() {
	atomic.StoreInt64(&self.end_time, 0)
	atomic.StoreInt64(&self.begin_time, 0)
	atomic.StoreInt32(&self.msg_send, 0)
	atomic.StoreInt32(&self.msg_succ, 0)
	atomic.StoreInt32(&self.msg_failed, 0)
	atomic.StoreInt32(&self.msg_error, 0)
}
