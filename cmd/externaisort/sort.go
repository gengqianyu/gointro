package main

import (
	"bufio"
	"fmt"
	"gointro/cmd/pipeline"
	"math"
	"os"
	"strconv"
)

func main() {
	//为什么要分 4 块是因为，cpu 是 4 核的，基于每一块都有一个 cpu 去处理
	//单机并发版
	//p := CreatePipeline("large.in", 4)
	//网络并发版
	p := CreateNetPipeline("large.in", 4)
	WriteToFile("large.out", p)
	PrintFile("large.out")

}

func PrintFile(fileName string) {
	file, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	p := pipeline.ReaderSource(bufio.NewReader(file), -1)
	//打印前一百个
	count := 0
	for v := range p {
		fmt.Println(v)
		count++
		if count == 100 {
			break
		}
	}

}

func WriteToFile(fileName string, pipelineChan <-chan int) {
	//创建文件
	file, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	//包装成一个buffer writer
	writer := bufio.NewWriter(file)
	defer writer.Flush()
	// 写数据
	pipeline.WriterSink(writer, pipelineChan)
}

// CreatePipeline create pipeline 文件名  分多少个块读取
func CreatePipeline(fileName string, chunkCount int) <-chan int {

	var chunkChannels []<-chan int

	fileInfo, err := os.Stat(fileName) //获取文件详情
	if err != nil {
		panic(err)
	}
	chunkSize := int(math.Ceil(float64(fileInfo.Size()) / float64(chunkCount))) //每一块的大小,不整除向上取整 Ceil 返回大于或等于 x 的最小整数值。
	// math.Floor() 向下取整函数
	//打开计时器
	pipeline.InitTime()

	// 分块读取文件
	for i := 0; i <= chunkCount; i++ {
		//每次循环都得打开文件
		file, err := os.Open(fileName)
		if err != nil {
			panic(err)
		}

		// Seek 将下一次 Read 或 Write on file 的偏移量设置为 offset， whence 从那开始：0 表示 offset 相对于文件的原点，1 表示相对于当前偏移量，2 表示相对于结尾。
		// 读取一块数据时，相对于文件起始原点的偏移位置
		file.Seek(int64(i*chunkSize), 0)

		reader := bufio.NewReader(file)
		// 将一块数据读入 pipeline
		p := pipeline.ReaderSource(reader, chunkSize)
		//接收 pipeline 中的数据进行排序，将排好序数据 放入另一个 pipeline
		p = pipeline.InMemSort(p)
		//分块进行排序 将排好序的 pipeline 添加到切片容器里
		chunkChannels = append(chunkChannels, p)
	}

	// 多路 channel 两两归并
	return pipeline.MergeN(chunkChannels...)
}

// CreateNetPipeline create pipeline 文件名  分多少个块读取
// 网络的扩展
//
//	Channel
//
// [goroutine]-------->[goroutine]
//
//	Channel						 		internet								Channel
//
// [InMemSort goroutine]-------->[Writer Sink goroutine]----------------->[Reader Source goroutine]-------->[Merge goroutine]
// [							节点1					]					[					节点2						]
//
//	在 golang 中网络和文件都包在一个接口里面，它们都叫一个 writer 和 reader。
func CreateNetPipeline(fileName string, chunkCount int) <-chan int {
	// 分块读取文件
	var netAddr []string

	//每一块的大小
	fileInfo, err := os.Stat(fileName)
	if err != nil {
		panic(err.(any))
	} //获取文件详情
	chunkSize := int(math.Ceil(float64(fileInfo.Size()) / float64(chunkCount))) //每一块的大小
	//打开计时器
	pipeline.InitTime()
	// 第一个服务端口
	port := 7000
	for i := 0; i <= chunkCount; i++ {
		//每次循环都得打开文件
		file, err := os.Open(fileName)
		if err != nil {
			panic(err.(any))
		}
		//将下一次在文件上读取或写入的偏移量设置为偏移量，whence解释：0表示相对于文件原点，1表示相对于当前偏移量，2表示相对于末尾。
		//设置下一次读取相较于文件原点的偏移量
		file.Seek(int64(i*chunkSize), 0)
		p := pipeline.ReaderSource(bufio.NewReader(file), chunkSize)

		//分块进行排序 写入网络
		addr := ":" + strconv.Itoa(port+i)
		pipeline.NetWorkSink(addr, pipeline.InMemSort(p))

		//将服务地址收集并返回
		netAddr = append(netAddr, addr)
	}
	// 如果要部署到很多台机器，上面部分可以做一个服务，分块读数据(这台机器可以只读取第 3 个 chunk)，内部排序，开 web server，写入网络

	// 下面部分做成另一个服务，从网络的各个 web server 节点读取数据，再 merge。
	// 包括 merge 节点也可以用我们的 NetWorkSink 和 NetWorkSource 去包装，包装完成之后 我们的 merge 节点也可以部署到多台机器上。
	var netChannel []<-chan int
	//连接 net server，接收数据 收集返回的 channel
	for _, address := range netAddr {
		netChannel = append(netChannel, pipeline.NetWorkSource(address))
	}

	// 多路channel 两两合并 最后返回一个channel
	return pipeline.MergeN(netChannel...)
}
