package pipeline

import (
	"bufio"
	"fmt"
	"os"
	"testing"
)

func TestArraySource(t *testing.T) {
	pipe := ArraySource(3, 2, 6, 7, 4)

	//复杂遍历 channel

	//for {
	//	// 如果 pipe 被 close 掉 ok 为 false
	//	if num, ok := <-pipe; ok {
	//		fmt.Println(num)
	//	} else {
	//		break
	//	}
	//}

	//简单遍历 channel
	// for range 一个 channel，如果 channel 没有数据 range 会阻塞当前 goroutine ，
	//直到此 channel 在其他 goroutine 中 被 close ，当前 goroutine 才会 break 跳出 for 循环。
	for v := range pipe {
		fmt.Println(v)
	}
}

func TestInMemSort(t *testing.T) {
	pipe := InMemSort(ArraySource(3, 2, 6, 7, 4))
	for v := range pipe { // 如果 pipe 中没有准备好的数据，程序就会一直在这里阻塞等待数据到来。
		fmt.Println(v)
	}
}

func TestMerge(t *testing.T) {
	pipe := Merge(InMemSort(ArraySource(3, 2, 6, 7, 4)), InMemSort(ArraySource(1, 8, 5, 0, 9)))
	for v := range pipe { // 如果 pipe 中没有准备好的数据，程序就会一直在这里阻塞等待数据到来。
		fmt.Println(v)
	}
}

func TestRandomSource(t *testing.T) {
	// 如果文件存在删除文件
	if ok, _ := pathIsExist("small.in"); ok {
		os.Remove("small.in")
	}

	//create file
	file, err := os.Create("small.in")
	defer file.Close()

	if err != nil {
		t.Error(err)
	}

	p := RandomSource(50)
	// writer.write
	writer := bufio.NewWriter(file)
	WriterSink(writer, p)
	writer.Flush()

	fileInfo, err := file.Stat()
	//fileInfo, err := os.Stat("small.in")
	if err != nil {
		t.Error(err)
	}
	if fileInfo.Size() != 400 {
		t.Errorf(" expected :the small.in size is 400 byte;actul: %d Byte", fileInfo.Size())
	}

	// open file
	file, err = os.Open("small.in")
	if err != nil {
		t.Error(err)
	}
	//reader.read
	p = ReaderSource(bufio.NewReader(file), 400)

	for v := range p {
		t.Log(v)
	}
}

func pathIsExist(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
