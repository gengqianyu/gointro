package main

import "fmt"

func main() {
	ch := CreatHelloWorldChan()
	// 不停的接收打印
	for {
		msg := <-ch
		fmt.Println(msg)
	}
}

func CreatHelloWorldChan() chan string {
	// create channel type of string
	ch := make(chan string)
	// 开100 个 goroutine  send continue hell world to ch channel

	//这里就是一个服务连接池模型 只要稍加改动 将下面的for 改为for range clients
	for i := 0; i < 100; i++ {
		//这里必须传i，值传递会复制i 否则循环会重置下面i值
		go func(num int) {
			// 不停的向 channel 发送 msg
			for {
				ch <- fmt.Sprintf("hello world %d", num)
			}
		}(i)
	}
	return ch
}
