package pipeline

import (
	"bufio"
	"net"
)

// NetWorkSink write to net
func NetWorkSink(addr string, in <-chan int) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	// 开一个服务就走了，为了让调它的人等待，所以开一个 goroutine 去等连接
	go func() {
		defer listener.Close()
		//等待一个连接
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}
		defer conn.Close()

		writer := bufio.NewWriter(conn)
		defer writer.Flush()

		// 将 pipeline 的数据写入网络
		WriterSink(writer, in)
	}()

}

// NetWorkSource read from net
func NetWorkSource(addr string) <-chan int {
	out := make(chan int)
	go func() {
		//拨号连接
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			panic(err)
		}
		defer conn.Close()
		//包装成一个buffer reader
		reader := bufio.NewReader(conn)
		//将数据 net reader
		p := ReaderSource(reader, -1)
		for v := range p {
			out <- v
		}
		close(out)
	}()

	return out
}
