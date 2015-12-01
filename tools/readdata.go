package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
)

const (
	STRING_KEY_LEN = 32
	STRING_VAL_LEN = 32 //actual   len is STRING_VAL_LEN + STRING_KEY_LEN
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
	outputFile, outputError := os.Open(filename)
	if outputError != nil {
		fmt.Printf("An error occurred with file opening or creation\n")
		return false
	}
	defer outputFile.Close()

	outputreader := bufio.NewReader(outputFile)

	for {
		data, inputerr := outputreader.ReadString('\n')
		if inputerr == io.EOF {
			break
		}
		datas := strings.Split(data, " ")
		if len(datas) < 1 {
			continue
		}
		fmt.Printf("key:%s, value:%s\n", datas[0], datas[1])
	}
	return true
}

func Init(testdata int, num int) bool {
	fmt.Printf(" concurent %d, datanum %d \n", num, testdata)
	wg := new(sync.WaitGroup)
	for i := 0; i < num; i++ {
		wg.Add(1)
		filename := fmt.Sprintf("testdata/qdb_data_%d.log", i)
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
