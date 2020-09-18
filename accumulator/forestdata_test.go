package accumulator

import (
	"fmt"
	"testing"
)

// newCowSimForest creates a CowForest for testing
func newCowSimForest(directory string) *Forest {
	f := new(Forest)
	f.numLeaves = 0
	f.rows = 0

	d, err := initialize("")
	if err != nil {
		panic(err)
	}
	f.data = d

	f.data.resize(1)
	f.positionMap = make(map[MiniHash]uint64)
	return f
}

func TestGPosToLocPos(t *testing.T) {
	pos := uint64(254)
	forestRows := uint8(7)
	treeBlockRow, offset, err := getTreeBlockPos(pos, forestRows)
	if err != nil {
		t.Fatal(err)
	}

	locRow, locPos := gPosToLocPos(pos, offset, treeBlockRow, forestRows)
	fmt.Printf("\nfor gPos:%d, treeBlockRow:%d, offset:%d, forestRows:%d -->\n"+
		"locRow:%d, locPos:%d\n", pos, treeBlockRow, offset, forestRows,
		locRow, locPos)
	fmt.Printf("\ntreeBlockRow: %d, offset: %d, err: %s\n",
		treeBlockRow, offset, err)

}

func TestGrabPrevMult(t *testing.T) {
	fmt.Println(grabPrevMult(0, leavesPerTreeTable))
	fmt.Println(grabPrevMult(1, leavesPerTreeTable))
	fmt.Println(grabPrevMult(leavesPerTreeTable, leavesPerTreeTable))
	fmt.Println(grabPrevMult(leavesPerTreeTable+1, leavesPerTreeTable))
	fmt.Println(grabPrevMult((leavesPerTreeTable*2)+1, leavesPerTreeTable))

	fmt.Println(grabPrevMult(254, leavesPerTreeTable))

	fmt.Println(grabPrevMult(65536, leavesPerTreeTable-1))
}

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
	pos := uint64(79)
	forestRows := uint8(17)
	treeBlockRow, offset, err := getTreeBlockPos(pos, forestRows)
	fmt.Printf("For pos: %d, forestRows: %d\n", pos, forestRows)
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
	fmt.Println(getRowOffset(0, 7))
	fmt.Println(getRowOffset(1, 7))
	fmt.Println(getRowOffset(2, 7))
	fmt.Println(getRowOffset(3, 7))
	fmt.Println(getRowOffset(4, 7))
	fmt.Println(getRowOffset(5, 7))
	fmt.Println(getRowOffset(6, 7))
	fmt.Println(getRowOffset(7, 7))

	fmt.Println()
	fmt.Println()
	fmt.Println()
	fmt.Println(getRowOffset(0, 16))
	fmt.Println(getRowOffset(1, 16))
	fmt.Println(getRowOffset(2, 16))
	fmt.Println(getRowOffset(3, 16))
	fmt.Println(getRowOffset(4, 16))
	fmt.Println(getRowOffset(5, 16))
	fmt.Println(getRowOffset(6, 16))
	fmt.Println(getRowOffset(7, 16))
	fmt.Println(getRowOffset(8, 16))
	fmt.Println(getRowOffset(9, 16))
	fmt.Println(getRowOffset(10, 16))
	fmt.Println(getRowOffset(11, 16))
	fmt.Println(getRowOffset(12, 16))
	fmt.Println(getRowOffset(13, 16))
	fmt.Println(getRowOffset(14, 16))
	fmt.Println(getRowOffset(15, 16))
}

func TestGetTreeBlockPosRange(t *testing.T) {
	posSameOffset, treeBlockOffsets, treeBlockRow := getTreeBlockPosRange(108, 3, 6)
	if len(posSameOffset) != len(treeBlockOffsets) {
		t.Fatal("different offset lengths")
	}
	fmt.Println("posSameOffset:", posSameOffset)
	fmt.Println("treeBlockOffsets:", treeBlockOffsets)
	fmt.Println("treeBlockRow:", treeBlockRow)
}

func TestGetTreeTablePos(t *testing.T) {
	row, offset := getTreeTablePos(255963, 19)
	fmt.Printf("row is: %d, offest is: %d\n", row, offset)
}
