package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
)

// flag 包是做命令行参数解析的
// 定义命令行参数
var port = flag.Int("port", 0, "http server port example 8888")

func main() {
	// 从 Args(参数) 中解析注册的 flag。
	//必须在所有 flag 都注册好而未访问其值时执行。未注册却使用 flag -help 时，会返回 ErrHelp。
	flag.Parse()

	if *port == 0 {
		fmt.Printf("must specify a port")
		return
	}

	// 注册 http handler function
	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		fmt.Fprintf(writer, "hello world %s", request.FormValue("name"))
	})
	// listen host start server 注意 port 是指针类型必须用 * 去读值 否则 invalid port
	err := http.ListenAndServe(":"+strconv.Itoa(*port), nil)
	if err != nil {
		log.Fatalln(err)
	}
}
