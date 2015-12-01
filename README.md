# coffo
coffo

redis压力测试工具

Installation

Simple as it takes to type the following command:

go get github.com/miffa/coffo


Usage

Usage: coffo [options...] ipaddr:port

Options:

  -n  Number of requests to run.
  
  -c  Number of requests to run concurrently. Total number of requests cannot
      be smaller than the concurency level.
      
  -t  Timeout in s.
  
  -type Object of redis (string hash set sortedset list)

  -cpus                 Number of used cpu cores.
                        (default for current machine is 1 cores)
                        
  -auth auth passwd, if empty , no auth
