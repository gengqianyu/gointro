/*
	归并排序回顾

		.将数据分为左右两半，分别归并排序，在把两个有序数据归并

		.如何归并：两个序列进行比较
		[1,3,6,7] [1,2,3,5] -> 1
		[3,6,7] [1,2,3,5] -> 1
		[3,6,7] [2,3,5] -> 2
		[3,6,7] [3,5] -> 3
		[6,7] [3,5] -> 3
		[6,7] [5] -> 5
		[6,7] [] -> 6
		[7] [] -> 7

						|		外部排序		 |
	[][][][][][][][][][]|[][][][][][][][][][]|[][][][][][][][][][][][]
						|					 |
		节点				|		节点			 |			节点

	外部排序数据流
	+------+--->+---+--->+---+--->+----+--->+----+
	[Source]--->[节点]--->[节点]--->[节点]--->[Sink]s
	+------+--->+---+--->+----+--->+---+--->+----+

		Source ：只出不进
		节点：多进多出
		Sink：只进不出

					节点组装
	传统语言				|				go语言
						|
	 方法调用			|				channel

[对象]------->[对象]		|	[goroutine]------->[goroutine]

	|
	|

外部排序 Pipeline

	+--->[读取]--->[内部排序]-----+			上游两路数据必须排好序才能进行归并
	|							|--->[归并]------+
	+--->[读取]--->[内部排序]-----+				|

[原始数据]---|											+--->[归并]--->[排序结果写文件]

	+--->[读取]--->[内部排序]-----+				|
	|							|--->[归并]------+
	+--->[读取]--->[内部排序]-----+

	+--->[ReadSource]--->[InMemSort]----+			上游两路数据必须排好序才能进行归并
	|									|--->[Merge]----+
	+--->[ReadSource]--->[InMemSort]----+				|

[Source]---|													+--->[Merge]--->[WriterSink]

	+--->[ReadSource]--->[InMemSort]----+				|
	|									|--->[Merge]----+
	+--->[ReadSource]--->[InMemSort]----+
*/
package main

import (
	"bufio"
	"fmt"
	"gointro/cmd/pipeline"
	"os"
)

func main() {
	//mergeDome()
	const fileName = "small.in"
	const num = 64
	//crate file
	file, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	// defer execute file close before exit
	defer file.Close()

	// generate data

	in := pipeline.RandomSource(num)
	// write data to file 为了更快的读写我们加一个 buffer 缓冲区
	//
	writer := bufio.NewWriter(file)
	pipeline.WriterSink(writer, in)
	writer.Flush() //将缓冲数据写入底层 io.Writer

	// open file
	file, err = os.Open(fileName)
	if err != nil {
		panic(err)
	}

	// 将 file 包装一下，加一个 buffer
	reader := bufio.NewReader(file)
	out := pipeline.ReaderSource(reader, -1)

	//打印前 100 个数据
	count := 0
	for v := range out {
		fmt.Println(v)
		count++
		if count == 100 {
			break
		}
	}
}

func mergeDome() {
	ch := pipeline.Merge(
		pipeline.InMemSort(pipeline.ArraySource(5, 6, 3, 4, 9, 8, 2)),
		pipeline.InMemSort(pipeline.ArraySource(7, 8, 0, 1, 4, 5, 6)),
	)
	//for {
	//	if element, ok := <-ch; ok {
	//		print(element)
	//	} else {
	//		break
	//	}
	//}
	//简单做法
	for e := range ch {
		fmt.Println(e)
	}
}
