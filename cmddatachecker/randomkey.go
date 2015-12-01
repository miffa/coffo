package cmddatachecker

import (
	"coffo/util"
	"math/rand"
	"sync/atomic"
)

//////////////////////////////////
type DataGen struct {
	//I32seed int32
	//I64seed int64
	//Strseed string
	//time seed
}

var (
	MyDataGen *DataGen = &DataGen{}
)

func (self *DataGen) Random32Key(seed int32) int32 {
	return rand.Int31n(seed)
}

func (self *DataGen) Random64Key(seed int64) int64 {
	return rand.Int63n(seed)
}

func (self *DataGen) RandomStringKey(leng uint) string {
	//return util.RandomString(leng)
	return util.RandomAscii(leng)
}

type UID struct {
	Uid int64
}

func (self *UID) GetUid() int64 {
	atomic.AddInt64(&self.Uid, 1)
	return atomic.LoadInt64(&self.Uid)
}

var MyUid *UID = &UID{Uid: MyDataGen.Random64Key(10000000000000)}
var MyIndex *UID = &UID{Uid: MyDataGen.Random64Key(10000)}
