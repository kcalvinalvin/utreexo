package accumulator

import (
	"fmt"
	"testing"
)

func TestGetPosInTreeBlock(t *testing.T) {
	num := getPosInTreeBlock(7999, 12, 0)
	fmt.Printf("for globalPos:%d and treeBlock:%d\n"+
		"pos is: %d\n", 7999, 12, num)

	n := getPosInTreeBlock(300, 6, 0)

	fmt.Printf("for globalPos:%d and treeBlock:%d\n"+
		"pos is: %d\n", 300, 6, n)

	x := getPosInTreeBlock(52, 0, 0)
	fmt.Printf("for globalPos:%d and treeBlock:%d\n"+
		"pos is: %d\n", 52, 0, x)

	fmt.Println(getPosInTreeBlock(17000, 12, 0))

}

func TestGetPosition(t *testing.T) {
	fmt.Println(getPosition(32, 7))
	fmt.Println(getPosition(128, 7))
	fmt.Println(getPosition(4, 3))
	fmt.Println(getPosition(20000000, 4))
	fmt.Println(getPosition(123124, 25))

	fmt.Println(getPosInTreeBlock(33, 0, 6))
	fmt.Println(getPosInTreeBlock(33, 0, 12))
}
