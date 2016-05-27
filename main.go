package main

import (
	"coffo/cmdrunner"
        "coffo/cmddatachecker"
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strings"

	"coffo/reporter"

	log "github.com/alecthomas/log4go"
)

var usage = `Usage: myexe [options...]  ipaddress

Options:
  -n  Number of requests to run.
  -c  Number of requests to run concurrently. Total number of requests cannot
      be smaller than the concurency level.
  -h  codis address as host:port.
  -loop run all cmd of redis
  -type redis data struct which we will test(sortedset)
  -cpus                 Number of used cpu cores.
  -t cmd period
  -auth auth of redis or codis
  (default for current machine is %d cores)
                        `

var (
	c      = flag.Int("c", 50, "")         //con_connect num
	n      = flag.Int("n", 1000000000, "") //req num
	t      = flag.Int("t", 3600, "")
	cpus   = flag.Int("cpus", runtime.GOMAXPROCS(-1), "")
	ifloop = flag.Bool("loop", false, "")
	//disableKeepAlives  = flag.Bool("disable-keepalive", false, "")
	data_type   = flag.String("type", "string", "")
	auth_passwd = flag.String("auth", "", "")
	filepath = flag.String("datapath", "tools/testdata", "")
)

func main() {
	flag.Usage = func() {
		fmt.Fprint(os.Stderr, fmt.Sprintf(usage, runtime.NumCPU()))
	}

	flag.Parse()
	if flag.NArg() < 1 {
		fmt.Fprintf(os.Stderr, " args num is %d\n", flag.NArg())
		usageAndExit("need ipaddress like this 23.45.66.78:80")
	}

	codisAddr := flag.Args()[0]
	if codisAddr == "" {
		usageAndExit("empty  ipaddress ")
	} else {
		fmt.Printf("codis client is %s\n", codisAddr)
	}

	if !checkIpAddr(codisAddr) {
		usageAndExit("invalid  ipaddress ")
	}

	runtime.GOMAXPROCS(*cpus)
	log.LoadConfiguration("./conf/log.xml")
	log.Info("con client is %d\n", *c)
	log.Info("request quantity is %d\n", *n)
	log.Info("data type is %s \n", *data_type)
	reporter.Dayreport.Init()
	reporter.Dayreport.Run()

	Runpprof()
        cmddatachecker.Filepath = *filepath
        fmt.Printf("data pth %s", cmddatachecker.Filepath)

	//c int32, req int32, ipandport string, port int16, sec int32, datatype string
	loadrunner := cmdrunner.NewCmdRunner(uint32(*c), int32(*n), codisAddr, 60, *data_type, *t, *auth_passwd)
	if *ifloop {
		loadrunner.RunLoop()
	} else {
		loadrunner.Run()
	}
}

func usageAndExit(message string) {
	if message != "" {
		fmt.Fprintf(os.Stderr, message)
		fmt.Fprintf(os.Stderr, "\n\n")
	}
	flag.Usage()
	fmt.Fprintf(os.Stderr, "\n")
	os.Exit(1)
}

func checkIpAddr(ipaddr string) bool {
	_, _, err := net.SplitHostPort(ipaddr)
	if err != nil {
		return false
	} else {
		return true
	}
}

//
func Runpprof() {
	var ipaddr string
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		panic(err)
	}

	for _, ip := range addrs {

		strip := strings.Split(ip.String(), "/")[0]
		if neiwang := strings.HasPrefix(strip, "10."); neiwang {
			ipaddr = strip
			break
		}
		if neiwang := strings.HasPrefix(strip, "192."); neiwang {
			ipaddr = strip
			break
		}
	}
	ipaddr = ipaddr + ":6060"
	go func(ip string) {
		http.ListenAndServe(ip, nil)
	}(ipaddr)
}
