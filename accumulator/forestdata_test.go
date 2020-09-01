package accumulator

import (
	"fmt"
	"testing"
)

/*
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
*/

/*
func TestGetPosition(t *testing.T) {
	fmt.Println(getPosition(100, 12))
	fmt.Println(detectRow(100, 6))
	/*
		fmt.Println(getPosition(32, 7))
		fmt.Println(getPosition(128, 7))
		fmt.Println(getPosition(4, 3))
		fmt.Println(getPosition(20000000, 4))
		fmt.Println(getPosition(123124, 25))

		fmt.Println(getPosInTreeBlock(33, 0, 6))
		fmt.Println(getPosInTreeBlock(33, 0, 12))
}
*/

func TestGetTreeBlockPos(t *testing.T) {
	//pos := uint64(4)
	//forestRows := uint8(3)
	//maxCachedTables := 1

	//pos := uint64(1040384)
	pos := uint64(254)
	forestRows := uint8(7)
	maxCachedTables := 1
	treeBlockRow, offset, err := getTreeBlockPos(
		pos, forestRows, maxCachedTables)
	fmt.Printf("For pos: %d, forestRows: %d, maxCachedTables: %d\n",
		pos, forestRows, maxCachedTables)
	fmt.Printf("treeBlockRow: %d, offset: %d, err: %s\n",
		treeBlockRow, offset, err)

}

func TestGetRowOffset(t *testing.T) {
	for forestRows := uint8(1); forestRows < 63; forestRows++ {
		for row := uint8(0); row <= forestRows; row++ {
			offset := getRowOffset(row, forestRows)

			switch row {
			case 0:
				if offset != 0 {
					t.Fatal()
				}
				break

			case 1:
				if offset != (1 << forestRows) {
					fmt.Println(offset, (1 << forestRows))
					t.Fatal()
				}
				break
			case 2:
				row1 := uint64(1 << forestRows)
				row2 := uint64(row1 + (row1 / 2))
				if offset != row2 {
					fmt.Println(row2, offset)
					t.Fatal()
				}
				break
				/*
					case 3:
						row1 := uint64(1 << forestRows)
						row2 := uint64(row1 + (row1 / 2))
						row3 := uint64(row2 + (row2 / 2))
						if offset != row3 {
							t.Fatal()
						}
						break
				*/
			}

		}
	}
	fmt.Println(getRowOffset(0, 6))
	fmt.Println(getRowOffset(1, 6))
	fmt.Println(getRowOffset(2, 6))
	fmt.Println(getRowOffset(3, 6))
	fmt.Println(getRowOffset(4, 6))
	fmt.Println(getRowOffset(5, 6))
	fmt.Println(getRowOffset(6, 6))

	fmt.Println(getRowOffset(7, 7))

	fmt.Println(getRowOffset(0, 19))
	fmt.Println(getRowOffset(1, 19))
	fmt.Println(getRowOffset(2, 19))
	fmt.Println(getRowOffset(3, 19))
	fmt.Println(getRowOffset(4, 19))
	fmt.Println(getRowOffset(5, 19))
	fmt.Println(getRowOffset(6, 19))
	fmt.Println(getRowOffset(7, 19))
	fmt.Println(getRowOffset(8, 19))
	fmt.Println(getRowOffset(9, 19))
	fmt.Println(getRowOffset(10, 19))
	fmt.Println(getRowOffset(11, 19))
	fmt.Println(getRowOffset(12, 19))
	fmt.Println(getRowOffset(13, 19))
	fmt.Println(getRowOffset(13, 19))
	fmt.Println(getRowOffset(14, 19))
	fmt.Println(getRowOffset(15, 19))
	fmt.Println(getRowOffset(16, 19))
	fmt.Println(getRowOffset(17, 19))
	fmt.Println(getRowOffset(18, 19))
	fmt.Println(getRowOffset(19, 19))
}
