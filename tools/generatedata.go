package main

import (
	"bufio"
	"coffo/cmddatachecker"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
)

const (
	STRING_KEY_LEN = 128
	STRING_VAL_LEN = 1024 //actual   len is STRING_VAL_LEN + STRING_KEY_LEN
)

var (
	c    = flag.Int("c", 50, "")   //con_connect num
	n    = flag.Int("n", 1000, "") //req num
	cpus = flag.Int("cpus", runtime.GOMAXPROCS(-1), "")
)

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(*cpus)
	Init(*n, *c)
}

func GenData2File(testdata int, filename string, wg *sync.WaitGroup) bool {
	// var outputWriter *bufio.Writer
	// var outputFile *os.File
	// var outputError os.Error
	// var outputString string
	defer wg.Done()
	outputFile, outputError := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0666)
	if outputError != nil {
		fmt.Printf("An error occurred with file opening or creation\n")
		return false
	}
	defer outputFile.Close()

	outputWriter := bufio.NewWriter(outputFile)

	//init test data
	//start := time.Now().UnixNano() //debug
	var testquantity int = 0
	for testquantity < testdata {
		index := cmddatachecker.MyIndex.GetUid()
		key := cmddatachecker.MyDataGen.RandomStringKey(STRING_KEY_LEN)
		key = key + strconv.FormatInt(index, 10)
		value := key + cmddatachecker.MyDataGen.RandomStringKey(STRING_VAL_LEN)
		outputString := fmt.Sprintf("%s %s\n", key, value)
		outputWriter.WriteString(outputString)
		if testquantity%100 == 0 {
			outputWriter.Flush()
		}
		testquantity++
	}
	outputWriter.Flush()

	//end := time.Now().UnixNano()
	//输出执行时间，单位为毫秒。
	//log.Info("StringGenerator init use time %d ms", (end-start)/1000000)
	//log.Info("here has [%d] args in it, MAX_TESTDATA[%d]\n", len(self.data_list), MAX_TESTDATA)
	return true
}

func Init(testdata int, num int) bool {
	fmt.Printf(" concurent %d, datanum %d \n", num, testdata)
	wg := new(sync.WaitGroup)
	for i := 0; i < num; i++ {
		wg.Add(1)
		filename := fmt.Sprintf("a/qdb_data_%d.log", i)
		//go GenData2File(testdata, filename, wg)
		GenData2File(testdata, filename, wg)
		fmt.Printf(" datafile %s is ok \n", filename)
	}
	wg.Wait()
	return true
}

func Openfile(index int) *os.File {
	filename := fmt.Sprintf("qdb_data_%d.log", index)
	outputFile, outputError := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0666)
	if outputError != nil {
		fmt.Printf("An error occurred with file opening or creation\n")
		return nil
	}
	return outputFile
}
