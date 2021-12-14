package pipeline

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"time"
)

var startTime time.Time

func InitTime() {
	startTime = time.Now()
}

// ArraySource 将数组中所有数据送进 channel； 返回一个，使用者只能接收 msg 的 channel of int ，创建者只能发送送 msg 的 channel of int
// 参数是一个可变参数，用...表示，用 range 接收
// 数组数据源节点 - channel 的关闭以及检测。
func ArraySource(a ...int) <-chan int {
	// create  channel type of int
	out := make(chan int)

	go func() {
		for _, v := range a {
			// send a element to channel
			out <- v
		}
		// close out of channel 数据发送完成需要 close channel
		// 一般来说 channel 是不需要 close 的，在并发编程中一般不需要 close，在并行计算的 pipeline 中有时候我们需要 close
		defer close(out)
	}()

	// return channel
	return out
}

// InMemSort 内部排序
// 从一个 channel 中接收 msg，组合成 slice 然后排序完成，再放入另一个 channel 中
// 接收一个只进不出的 channel，返回一个只出不进的 channel
func InMemSort(in <-chan int) <-chan int {
	// create a channel type of int and add buffer
	out := make(chan int, 1024)

	// open a goroutine
	go func(p <-chan int) {

		//read into memory
		var es []int
		for e := range p {
			es = append(es, e)
		}
		//记录读取时间
		fmt.Println("Read done:", time.Now().Sub(startTime))

		//slice sort
		sort.Ints(es)

		fmt.Println("InMemSort done:", time.Now().Sub(startTime))

		// 重新发送排好序的数据
		for _, e := range es {
			out <- e
		}
		close(out)
	}(in)

	// return channel
	return out
}

// Merge 合并归并节点 二合一
// 接收将两路输入 channel，返回一个输出 channel
func Merge(in1, in2 <-chan int) <-chan int {
	// create a channel type of int and add buffer
	out := make(chan int, 1024)

	go func() {
		//Go 语言在最初版本使用 x, ok := <-c 实现非阻塞的收发，以下是与非阻塞收发相关的提交：

		//1.select default 提交支持了 select 语句中的 default；
		//2.gc: special case code for single-op blocking and non-blocking selects 提交引入了基于 select 的非阻塞收发。
		//3.gc: remove non-blocking send, receive syntax 提交将 x, ok := <-c 非阻塞接收语法删除；
		//4.gc, runtime: replace closed(c) with x, ok := <-c 提交使用 x, ok := <-c 语法替代 closed(c)  语法判断 Channel 的关闭状态；
		//																	  注意:这里不是 close(c) 函数

		// 那边排序完成，这边才能接
		// 下面这种，分别接一下，其实就做到了，等待两边分别完成内部排序的动作
		// 等上游的数据 （这里有阻塞，和常规的阻塞队列并无不同）
		// ok 用于判断 channel 是否被关闭，true 表示 channel 未关闭，false 表示 channel 已关闭。不再表示非阻塞接收是否有数据了。
		v1, ok1 := <-in1
		v2, ok2 := <-in2

		// 这里相当于，两个队列的数据比较，将排序数据发送给接收方
		// 一次比较只会发送一个数据，留下的下次接着比较
		for ok1 || ok2 {
			// in2 被关闭了，里面没有数据了，当然要将 in1 的 v1 输出
			// 如果 channel2 已关闭 ，或者 channel2 未关闭，那么就要保证 channel1 也得未关闭有数据，同时 channel1 的值小于 channel2 的值
			if !ok2 || (ok1 && v1 <= v2) {
				out <- v1
				v1, ok1 = <-in1
			} else {
				out <- v2
				v2, ok2 = <-in2
			}
		}
		//如果外面一直在收，这里就应该一直不停的发，如果不想一直发那就要 close
		//告诉外面我没数据了，你不要再等我发了。
		close(out)

		fmt.Println("Merge done:", time.Now().Sub(startTime))
	}()
	return out
}

// MergeN 合并多路 channel 两两归并
func MergeN(inputs ...<-chan int) <-chan int {
	if len(inputs) == 1 {
		return inputs[0]
	}
	m := len(inputs) >> 1
	//两个半开半闭期间 inputs[0..m] and inputs[m..end]

	//这两个输入分别是一个 channel，把这两个 channel merge 起来
	// 递归的合并channel，最后成两个了合并成一个，返回出去
	return Merge(MergeN(inputs[:m]...), MergeN(inputs[m:]...))
}

// ReaderSource reader
// read data from reader 分块读取，每一块数据最大是 chunkSize
func ReaderSource(reader io.Reader, chunkSize int) <-chan int {
	// create a channel type of int and add buffer
	// channel received and send is block,so add buffer(缓冲)
	// 生产者往 channel 里发一个消息，必须得有消费者去收。如果没人收就会阻塞
	// 创建 channel 时，加第二个参数，生产者往 channel 里发 1024 个消息，再让消费者去收，这样就大大提高了效率
	out := make(chan int, 1024)

	go func() {
		// 1个 byte = 8 bit(位) 8个 byte 正好可以表示一个 64 位 int 型
		// 我们要处理的是 int64 类型，因此缓冲区开 8 个字节
		buffer := make([]byte, 8)
		bytesRead := 0
		//continue read from reader
		for {
			// n 表示读了多少字节，err 表示错误,  读到 EOF 就会有错误；reader 把数据读入 buffer
			n, err := reader.Read(buffer)
			bytesRead += n

			// read 数据有可能不够 8 个字节也是有错误返回的，所以这里先处理数据再处理错误
			if n > 0 {
				//按照大端字节序将 buffer 中二进制数据转成一个 64 位无符号 int 整数
				v := int(binary.BigEndian.Uint64(buffer))
				out <- v
			}
			//读到 EOF 就会有错误；chunkSize 为 -1 表示全部读，当读取的字节数大于等于分块大小的时候说明分块已读完。
			if err != nil || (chunkSize != -1 && bytesRead >= chunkSize) {
				break
			}
		}
		close(out)
	}()

	return out
}

// WriterSink writer
//write data to the writer
func WriterSink(writer io.Writer, in <-chan int) {
	for v := range in {

		//创建一个 buffer ,为了提高效率先将数据写入 Buffer
		buffer := make([]byte, 8)

		//将 v 使用大端字节序，转城二进制，并向 buffer 写入二进制数据
		binary.BigEndian.PutUint64(buffer, uint64(v))

		//writer 将 buffer 中的数据写入io.writer
		writer.Write(buffer)
	}
}

// RandomSource
// rand num
func RandomSource(count int) <-chan int {
	out := make(chan int)

	go func() {
		for i := 0; i < count; i++ {
			out <- rand.Int()
		}
		close(out)
	}()

	return out
}
