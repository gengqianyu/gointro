package main

import (
	"fmt"
	"sort"
)

func main() {
	// create a slice type of int
	a := []int{4, 6, 7, 8, 9, 2, 3, 5, 1}
	sort.Ints(a)
	for i, v := range a {
		fmt.Println(i, v)
	}
}
