package accumulator

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
)

// leafSize is a [32]byte hash (sha256).
// Length is always 32.
const leafSize = 32

// ForestData is the thing that holds all the hashes in the forest.  Could
// be in a file, or in ram, or maybe something else.
type ForestData interface {
	// returns the hash value at the given position
	read(pos uint64) Hash

	// writes the given hash at the given position
	write(pos uint64, h Hash)

	// for the given two positions, swap the hash values
	swapHash(a, b uint64)

	// given positions a and b, take the width value (w) and swap
	// all the positions widthin it.
	swapHashRange(a, b, w uint64)

	// returns how many leaves the current forest can hold
	size() uint64

	// allocate more space to the forest. newSize should be in leaf count (bottom row of the forest)
	// can't resize down
	resize(newSize uint64) // make it have a new size (bigger)

	setRow(row uint8)

	// closes the forest-on-disk for stopping
	close()
}

// ********************************************* forest in ram

type ramForestData struct {
	// m []Hash
	m []byte
}

// TODO it reads a lot of empty locations which can't be good

// reads from specified location.  If you read beyond the bounds that's on you
// and it'll crash
func (r *ramForestData) read(pos uint64) (h Hash) {
	// if r.m[pos] == empty {
	// 	fmt.Printf("\tuseless read empty at pos %d\n", pos)
	// }
	pos <<= 5
	copy(h[:], r.m[pos:pos+32])
	return
}

// writeHash writes a hash.  Don't go out of bounds.
func (r *ramForestData) write(pos uint64, h Hash) {
	// if h == empty {
	// 	fmt.Printf("\tWARNING!! write empty at pos %d\n", pos)
	// }
	pos <<= 5
	copy(r.m[pos:pos+32], h[:])
}

// TODO there's lots of empty writes as well, mostly in resize?  Anyway could
// be optimized away.

// swapHash swaps 2 hashes.  Don't go out of bounds.
func (r *ramForestData) swapHash(a, b uint64) {
	r.swapHashRange(a, b, 1) // just calls swap range..
}

// swapHashRange swaps 2 continuous ranges of hashes.  Don't go out of bounds.
// fast but uses more ram
func (r *ramForestData) swapHashRange(a, b, w uint64) {
	// fmt.Printf("swaprange %d %d %d\t", a, b, w)
	a <<= 5
	b <<= 5
	w <<= 5
	temp := make([]byte, w)
	copy(temp[:], r.m[a:a+w])

	copy(r.m[a:a+w], r.m[b:b+w])
	copy(r.m[b:b+w], temp[:])
}

// size gives you the size of the forest
func (r *ramForestData) size() uint64 {
	return uint64(len(r.m) / 32)
}

// resize makes the forest bigger (never gets smaller so don't try)
func (r *ramForestData) resize(newSize uint64) {
	r.m = append(r.m, make([]byte, (newSize-r.size())*leafSize)...)
}

func (r *ramForestData) close() {
	// nothing to do here fro a ram forest.
}

func (r *ramForestData) setRow(row uint8) {
}

// ********************************************* forest on disk

// rowPerTreeBlock is the rows a treeBlock holds
// 7 is chosen as a tree with height 6 will contain 7 rows (row 0 to row 6)
// TODO duplicate of Config
const rowPerTreeBlock = 7

//const rowPerTreeBlock = 3

// This is the same concept as forestRows, except for treeBlocks.
// Fixed as a treeBlockRow cannot increase. You just make another treeBlock if the
// current one isn't enough.
const treeBlockRows = 6

//const treeBlockRows = 2

// Number for the amount of treeBlocks to go into a table
const treeBlockPerTable = 1024

//const treeBlockPerTable = 32

// Number of leaves that a treeBlock holds
const leavesPerTreeBlock = 1 << treeBlockRows

// Number of leaves that a treeTable holds
const leavesPerTreeTable = leavesPerTreeBlock * treeBlockPerTable

// extension for the forest files on disk. Stands for, "Utreexo Forest
// On Disk
var extension string = ".ufod"

var (
	ErrorCorruptManifest = errors.New("Manifest is corrupted. Recovery needed")
)

func errorCorruptManifest() error { return ErrorCorruptManifest }

// metadata holds the temporary data about the CowForest that isn't saved
// to disk
type metadata struct {
	// fBasePath is the base directory for the .ufod files
	fBasePath string

	// staleFiles are the files that are not part of the latest forest state
	// these should be cleaned up.
	staleFiles []uint64
}

// manifest is the structure saved on disk for loading the current
// utreexo forest
// FIXME fix padding
type manifest struct {
	// The current allocated rows in forest
	forestRows uint8

	// The current allocated treeBlockRows in the CowForest
	treeBlockRows uint8

	// The latest synced Bitcoin block height
	currentBlockHeight int32

	// The number following 'MANIFEST'
	// A new one will be written on every db open
	currentManifestNum uint64

	// The current .ufod file number. This is incremented on every
	// new treeTable
	fileNum uint64

	// The latest synced Bitcoin block hash
	currentBlockHash Hash

	// location holds the on-disk fileNum for the treeTables. 1st array
	// holds the treeBlockRow info and the seoncd holds the offset
	location [][]uint64
}

// commit creates a new manifest version and commits it and removes the old manifest
// The commit is atomic in that only when the commit was successful, the
// old manifest is removed.
func (m *manifest) commit(basePath string) error {
	manifestNum := m.currentManifestNum + 1
	fName := fmt.Sprintf("MANIFEST-%d", manifestNum)
	fPath := filepath.Join(basePath, fName)

	// Create new manifest on disk
	fNewManifest, err := os.OpenFile(fPath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	var locLength int
	for _, row := range m.location {
		locLength += len(row)
	}

	// This is the bytes to be written
	var buf []byte //+locLength) // locLength is variable

	// 1. Append forestRows
	buf = append(buf, byte(m.forestRows))

	fmt.Println("buf len1 ", len(buf))

	// 2. Append currentBlockHeight
	var bHeight [4]byte
	binary.LittleEndian.PutUint32(bHeight[:], uint32(m.currentBlockHeight))
	buf = append(buf, bHeight[:]...)

	fmt.Println("buf len2 ", len(buf))
	fmt.Println(buf)

	// 3. Append fileNum
	var fNum [8]byte
	binary.LittleEndian.PutUint64(fNum[:], uint64(m.fileNum))
	buf = append(buf, fNum[:]...)

	fmt.Println("buf len3 ", len(buf))
	fmt.Println(buf)

	// 4. Append currentBlockHash
	buf = append(buf, m.currentBlockHash[:]...)
	fmt.Println(buf)

	fmt.Println("buf len4 ", len(buf))
	fmt.Println("curBH", m.currentBlockHash)

	fmt.Println(m.location)
	// 5. Append locations
	for _, row := range m.location {
		// append the length of the row
		uint32Buf := make([]byte, 4)

		binary.LittleEndian.PutUint32(uint32Buf[:], uint32(len(row)))
		buf = append(buf, uint32Buf...)

		// append the actual row
		rowBytes := []byte{}

		for _, element := range row {
			uint64Buf := make([]byte, binary.MaxVarintLen64)
			binary.LittleEndian.PutUint64(uint64Buf, element)
			rowBytes = append(rowBytes, uint64Buf...)
		}

		buf = append(buf, rowBytes...)
	}

	fmt.Println(len(buf))
	_, err = fNewManifest.Write(buf)
	if err != nil {
		return err
	}

	// Overwrite the current manifest number in CURRENT
	curFileName := filepath.Join(basePath, "CURRENT")
	fCurrent, err := os.OpenFile(curFileName, os.O_CREATE|os.O_WRONLY, 0655)
	if err != nil {
		return err
	}
	fNameByteArray := []byte(fName)
	_, err = fCurrent.WriteAt(fNameByteArray, 0)
	if err != nil {
		return err
	}

	// Remove old manifest
	fOldName := fmt.Sprintf("MANIFEST-%d", m.currentManifestNum)
	os.Remove(fOldName)
	/*
		if err != nil {
			// ErrOldManifestNotRemoved
			return err
		}
	*/

	return nil
}

// load loades the manifest from the disk
func (m *manifest) load(path string) error {
	curFileName := filepath.Join(path, "CURRENT")

	curFile, err := os.OpenFile(curFileName, os.O_RDONLY, 0444)
	if err != nil {
		return err
	}

	//buf := make([]byte, 37)
	manifestBytes, err := ioutil.ReadAll(curFile)
	if err != nil {
		return err
	}

	maniFilePath := filepath.Join(path, string(manifestBytes[:]))

	maniFile, err := os.Open(maniFilePath)
	if err != nil {
		return err
	}

	// 45 bytes are all that's needed to load except for the locations
	buf := make([]byte, 45)

	//buf, err := ioutil.ReadAll(maniFile)
	_, err = maniFile.Read(buf)
	if err != nil {
		return err
	}
	fmt.Println(buf)
	fmt.Println(len(buf))

	// 1. Read forestRows
	m.forestRows = uint8(buf[0])
	fmt.Println("forestRows:", m.forestRows)

	// 2. Read currentBlockHeight
	m.currentBlockHeight = int32(binary.LittleEndian.Uint32(buf[1:5]))
	fmt.Println("currentBlockHeight:", m.currentBlockHeight)

	// 3. Read fileNum
	m.fileNum = binary.LittleEndian.Uint64(buf[5:13])
	fmt.Println("fileNum", m.fileNum)

	// 4. Read currentBlockHash
	copy(m.currentBlockHash[:], buf[13:45])
	fmt.Println("curBlockH", m.currentBlockHash)

	var treeBlockRow int
	// 5. Append locations
	for {
		sizeBuf := make([]byte, 4)

		_, err := maniFile.Read(sizeBuf)
		if err != nil {
			if err == io.EOF {
				fmt.Println("EOF")
				break
			}
			return err
		}
		m.location = append(m.location, []uint64{})

		rowSize := binary.LittleEndian.Uint32(sizeBuf)
		fmt.Println("rowsize", rowSize)
		rowBytes := make([]byte, rowSize*binary.MaxVarintLen64)

		_, err = maniFile.Read(rowBytes)
		if err != nil {
			return err
		}

		for i := uint32(0); i < rowSize; i++ {
			//m.location[treeBlockRow][i] = binary.LittleEndian.Uint64(rowBytes[i : i+10])
			m.location[treeBlockRow] = append(m.location[treeBlockRow],
				binary.LittleEndian.Uint64(rowBytes[i:i+10]))
		}
		treeBlockRow++
	}
	fmt.Println(m.location)

	return nil
}

// treeBlock is a representation of a row 6 utreexo tree.
// It contains 127 leaves. The leaves can be a UTXO hash or another treeBlock
type treeBlock struct {
	// leaves are the utreexo nodes that are either
	// 1: Hash of a UTXO
	// 2: Parent hash of two nodes
	//
	// Able to hold up to 64 leaves
	leaves [127]Hash
}

// converts a treeBlock to byte slice
func (tb *treeBlock) serialize() []byte {
	// 127 nodes in a treeBlock, 32 bytes per sha256
	buf := make([]byte, 127*32)
	//fmt.Println(tb.leaves)
	for _, leaf := range tb.leaves {
		buf = append(buf, leaf[:]...)
	}
	return buf
}

// getTreeBlockPos grabs the relevant treeBlock position.
func getTreeBlockPos(pos uint64, forestRows uint8) (
	treeBlockRow uint8, treeBlockOffset uint64, err error) {

	// For testing. These should be set within the cowforest
	//rowPerTreeBlock := uint8(3)
	//TreeBlockRows := uint8(2)

	// maxPossiblePosition is the upper limit for the position that a given tree
	// can hold. Should error out if a requested position is greater than it.
	maxPossiblePosition := getRowOffset(forestRows, forestRows)
	if pos > maxPossiblePosition {
		err = fmt.Errorf("Position requested is more than the forest can hold\n"+
			"Requested: %d, MaxPossible: %d, forestRows: %d\n",
			pos, maxPossiblePosition, forestRows)
		return
	}

	// The row that the current position is on
	row := detectRow(pos, forestRows)

	// treeBlockRow is the "row" representation of a treeBlock forestRows
	// treeBlockRow of 0 indicates that it's the bottommost treeBlock.
	// Our current version has forestRows of 6 for the bottommost and 13
	// for the treeBlock above that. This is as there are 7 rows per treeBlock
	// ex: 0~6, 7~13, 14~20
	treeBlockRow = row / rowPerTreeBlock

	// get position relevant to the row, not the entire forest
	// ex:
	// 06
	// |-------\
	// 04      05
	// |---\   |---\
	// 00  01  02  03
	//
	// row 0 stays the same. Everything else changes
	// position 04 -> 00, 05 -> 01, 06 -> 00
	rowOffset := getRowOffset(row, forestRows)
	localRowPos := pos - rowOffset
	//fmt.Println("localRowPos", localRowPos)

	leafCount := 1 << forestRows       // total leaves in the forest
	nodeCountAtRow := leafCount >> row // total nodes in this row
	//treeBlockCountAtRow := nodeCountAtRow / (1 << 2) // change 2 to the relevant forestRows
	nodeCountAtTreeBlockRow := leafCount >> (treeBlockRow * rowPerTreeBlock) // total nodes in this TreeBlockrow
	//fmt.Println("nodeCountAtTreeBlockRow:", nodeCountAtTreeBlockRow)

	// If there are less nodes at row than a treeBlock holds, it means that there
	// are empty leaves in that single treeBlock
	var treeBlockCountAtRow int
	if nodeCountAtTreeBlockRow < (1 << treeBlockRows) {
		// Only 1 treeBlock, the top treeBlock, may be sparse.
		treeBlockCountAtRow = 1
	} else {
		//treeBlockCountAtRow = nodeCountAtRow / (1 << TreeBlockRows)
		treeBlockCountAtRow = nodeCountAtTreeBlockRow / (1 << treeBlockRows)
	}
	//fmt.Println("treeBlockCountAtRow:", treeBlockCountAtRow)

	// In a given row, how many leaves go into a treeBlock of that row?
	// For exmaple, a forest with:
	// row = 1, rowPerTreeBlock = 3
	// maxLeafPerTreeBlockAtRow = 2
	//
	// another would be
	// row = 0, rowPerTreeBlock = 3
	// maxLeafPerTreeBlockAtRow = 4
	maxLeafPerTreeBlockAtRow := nodeCountAtRow / treeBlockCountAtRow
	//fmt.Println("maxLeafPerTreeBlockAtRow", maxLeafPerTreeBlockAtRow)

	treeBlockOffset = localRowPos / uint64(maxLeafPerTreeBlockAtRow)

	return
}

// getRowOffset returns the first position of that row
// ex:
// 14
// |---------------\
// 12              13
// |-------\       |-------\
// 08      09      10      11
// |---\   |---\   |---\   |---\
// 00  01  02  03  04  05  06  07
//
// 8 = getRowOffset(1, 3)
// 12 = getRowOffset(2, 3)
func getRowOffset(row, forestRows uint8) uint64 {
	var offset uint64
	leaves := uint64(1 << forestRows)

	for i := uint8(0); i < row; i++ {
		offset += leaves
		leaves /= 2
	}
	return offset
}

// A wrapper around getTreeBlockPos for multiple nodes
func getTreeBlockPosRange(pos, width uint64, rows uint8) ([][]uint64, []uint64, uint8) {
	// check if the positions are at different treeBlocks
	// TODO maybe there is a better way then checking everything
	var (
		treeBlockOffsets []uint64
		posSameOffset    [][]uint64
		treeBlockRow     uint8 // only need for sanity checking
	)

	for i := pos; i <= pos+width; i++ {
		row, treeBlockOffset, err := getTreeBlockPos(i, rows)
		if err != nil {
			//return Hash{}, err
			panic(err)
		}

		// for the first loop. TODO Sorta ugly?
		if i == pos {
			treeBlockOffsets = append(treeBlockOffsets, treeBlockOffset)
			posSameOffset = append(posSameOffset, []uint64{i})

			// This is only needed for a sanity check
			treeBlockRow = row
		} else {
			// if offset is different add it
			if treeBlockOffsets[len(treeBlockOffsets)-1] != treeBlockOffset {
				treeBlockOffsets = append(treeBlockOffsets, treeBlockOffset)
				posSameOffset = append(posSameOffset, []uint64{i})
			} else {
				posSameOffset[len(posSameOffset)-1] = append(
					posSameOffset[len(posSameOffset)-1], i)
			}
		}
		// should never happen since transform is only done on the same row
		// only here for sanity checking for now
		if treeBlockRow != row {
			panic("swapHashRange called on different rows")
		}

	}
	return posSameOffset, treeBlockOffsets, treeBlockRow
}

// Translate a global position to its local position. This is the leaf position
// inside a treeBlock. If a treeBlock is of forestRows 6, it's a range of
// 0-126
func gPosToLocPos(gPos, offset uint64, treeBlockRow, forestRows uint8) (
	uint8, uint64) {
	// Sanity check to see if called with something it can't hold
	if gPos > getRowOffset(forestRows, forestRows) {
		s := fmt.Errorf("pos of %d is greater than the max of what forestRows"+
			"%d can hold\n", gPos, forestRows)
		panic(s)
	}

	// which row is the node in in the entire forest?
	globalRow := detectRow(gPos, forestRows)

	// the first position in the globalRow
	rowOffset := getRowOffset(globalRow, forestRows)

	//
	rowPos := gPos - rowOffset

	// total leaves in the forest
	leafCount := 1 << forestRows

	// total nodes in this row
	nodeCountAtRow := leafCount >> globalRow
	//fmt.Println("nodeCountAtRow", nodeCountAtRow)

	// total nodes in this treeBlockRow
	nodeCountAtTreeBlockRow := leafCount >> (treeBlockRow * rowPerTreeBlock)
	//fmt.Println("nodeCountAtTreeBlockRow", nodeCountAtTreeBlockRow)

	// This doesn't work for the root
	//treeBlockCountAtRow := nodeCountAtTreeBlockRow / (1 << treeBlockRows)

	// If there are less nodes at row than a treeBlock holds, it means that there
	// are empty leaves in that single treeBlock
	var treeBlockCountAtRow int
	if nodeCountAtTreeBlockRow < (1 << treeBlockRows) {
		// Only 1 treeBlock, the top treeBlock, may be sparse.
		treeBlockCountAtRow = 1
	} else {
		//treeBlockCountAtRow = nodeCountAtRow / (1 << TreeBlockRows)
		treeBlockCountAtRow = nodeCountAtTreeBlockRow / (1 << treeBlockRows)
	}
	//fmt.Println("treeBlockCountAtRow", treeBlockCountAtRow)

	rowBlockOffset := offset * uint64(nodeCountAtRow/treeBlockCountAtRow)
	//fmt.Println("rowBlockOffset", rowBlockOffset)
	//fmt.Println("nodeCountAtTreeBlockRow", nodeCountAtTreeBlockRow)
	locPos := rowPos - rowBlockOffset
	locRow := (globalRow - (rowPerTreeBlock * treeBlockRow))

	//fmt.Println("rowPos", rowPos)
	/*
			if locRow > 6 {
				s := fmt.Sprintln("gpos", gPos)
				s += fmt.Sprintln("treeBlockOffset", offset)
				s += fmt.Sprintln("treeBlockRow", treeBlockRow)
				s += fmt.Sprintln("forestRows", forestRows)
				s += fmt.Sprintln("locRow", locRow)
				s += fmt.Sprintln("locPos", locPos)
				panic("locRow is greater than 6" + s)
			}

		if locPos > 63 {
			// uint64 underflows. No position can be greater than 63 in
			// a treeBlock anyways
			locPos = 0
			//s := fmt.Sprintln("gpos", gPos)
			//s += fmt.Sprintln("treeBlockOffset", offset)
			//s += fmt.Sprintln("treeBlockRow", treeBlockRow)
			//s += fmt.Sprintln("forestRows", forestRows)
			//s += fmt.Sprintln("locRow", locRow)
			//s += fmt.Sprintln("locPos", locPos)
			//panic("locPos is greater than 63" + s)
		}
	*/

	return locRow, locPos
}

// treeTable is a group of treeBlocks that are sorted on disk
// A treeTable only contains the treeBlocks that are of the same row
// The included treeBlocks are sorted then stored onto disk
type treeTable struct {
	//treeBlockRow uint8
	// memTreeBlocks is the treeBlocks that are stored in memory before they are
	// written to disk. This is helpful as older treeBlocks get less and
	// less likely to be accessed as stated in 5.7 of the utreexo paper
	// NOTE 1024 is the current value of stored treeBlocks per treeTable
	// this value may change/can be changed
	memTreeBlocks [treeBlockPerTable]*treeBlock
}

func (tt *treeTable) serialize() (uint16, []byte) {
	// 127 nodes per treeBlock, 1024 treeBlocks in a treeTable, 32 byte hashes
	buf := make([]byte, 127*1024*32)
	//fmt.Println(tt.memTreeBlocks)
	var treeBlockCount uint16
	for _, tb := range tt.memTreeBlocks {
		if tb == nil {
			//fmt.Println("TREEBLOCK IS NIL")
			break
		}
		treeBlockCount++

		buf = append(buf, tb.serialize()...)
	}
	return treeBlockCount, buf
}

func newTreeTable() *treeTable {
	memBlocks := make([]*treeBlock, treeBlockPerTable)
	tt := new(treeTable)
	copy(tt.memTreeBlocks[:], memBlocks)
	return tt
}

// getTreeTablePos grabs the relevant treeBlock position.
func getTreeTablePos(pos uint64, forestRows uint8) (
	treeBlockRow uint8, treeTableOffset uint64) {
	// The row that the current position is on
	row := detectRow(pos, forestRows)

	// treeBlockRow is the "row" representation of a treeBlock forestRows
	// treeBlockRow of 0 indicates that it's the bottommost treeBlock.
	// Our current version has forestRows of 6 for the bottommost and 13
	// for the treeBlock above that. This is as there are 7 rows per treeBlock
	// ex: 0~6, 7~13, 14~20
	treeBlockRow = row / rowPerTreeBlock

	rowOffset := getRowOffset(row, forestRows)
	locRowPos := pos - rowOffset

	// The -1 is really one of the ugliest part of how utreexo
	// works because a treeTable holds positions 0~65535,
	// resulting in holding 65536 leaves
	treeTableOffset = grabPrevMult(locRowPos, leavesPerTreeTable-1) / (leavesPerTreeTable - 1)

	return
}

// Rounds down n to the nearest multiple of m. Returns 0 for any n <= m
func grabPrevMult(n uint64, m uint64) uint64 {
	// if it's a multiple, just subtract
	if n%m == 0 {
		if n == 0 {
			return 0
		}
		return n - m
	}

	//fmt.Println("n", n)
	//fmt.Println("n % m", n%m)
	//fmt.Println("n - (n % m) ", n-(n%m))
	up := m - (n % m)
	//fmt.Println("up", up)
	n += up
	return n - m
}

// Shorthand for copy-on-write. Unfortuntely, it doesn't go moo
type cowForest struct {
	// cachedTreeTables are the in-memory tables that are not yet committed to disk
	// TODO flush these after a certain number is in memory
	cachedTreeTables map[uint64]*treeTable

	// all the data that isn't saved to disk
	meta metadata

	// manifest contains all the necessary metadata for fetching
	// utreexo nodes
	manifest manifest
}

// initalize returns a cowForest with a maxCachedTables value set
func initialize(path string) (*cowForest, error) {
	cow := cowForest{
		meta: metadata{fBasePath: path},
	}
	cow.cachedTreeTables = make(map[uint64]*treeTable)
	cow.manifest.location = append(cow.manifest.location, []uint64{})

	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		panic(err)
	}
	return &cow, nil
}

// loads an existing cowForest
func load(path string) (*cowForest, error) {
	maniToLoad := new(manifest)

	err := maniToLoad.load(path)
	if err != nil {
		return nil, err
	}

	m := metadata{fBasePath: path}
	cow := cowForest{
		manifest: *maniToLoad,
		meta:     m,
	}

	cow.cachedTreeTables = make(map[uint64]*treeTable)

	return &cow, nil
}

// Read takes a position and forestRows to return the Hash of that leaf
func (cow *cowForest) read(pos uint64) Hash {
	//if pos == 81903 {
	//	//fmt.Println("READ CALLED on pos: ", pos)
	//}
	// Steps for Read go as such:
	//
	// 1. Fetch the relevant treeTable/treeBlock
	// 	a. Check if it's in memory. If not, go to disk
	// 2. Fetch the relevant treeBlock
	// 3. Fetch the leaf

	treeBlockRow, treeBlockOffset, err := getTreeBlockPos(pos, cow.manifest.forestRows)
	if err != nil {
		//return Hash{}, err
		panic(err)
	}
	_, treeTableOffset := getTreeTablePos(pos, cow.manifest.forestRows)

	// grab the treeTable location. This is just a number for the .ufod file
	location := cow.manifest.location[treeBlockRow][treeTableOffset]

	// check if it exists in memory
	table, found := cow.cachedTreeTables[location]

	// Table is not in memory
	if !found {
		//fmt.Println("TABLE NOT FOUND IN MEMORY")
		// Load the treeTable onto memory. This maps the table to the location
		err = cow.load(location)
		if err != nil {
			//return Hash{}, err
			panic(err)
		}
		table = cow.cachedTreeTables[location]
	}

	//fmt.Println("IS TABLE FOUND?", found)
	//fmt.Println(table.memTreeBlocks)
	//fmt.Println("FETCH", treeBlockOffset%treeBlockPerTable)
	tb := table.memTreeBlocks[treeBlockOffset%treeBlockPerTable]
	if tb == nil {
		//fmt.Println("TREEBLOCK IS NIL")

		tb = new(treeBlock)
	}

	locRow, localPos := gPosToLocPos(
		pos, treeBlockOffset, treeBlockRow, cow.manifest.forestRows)
	fetch := localPos + getRowOffset(locRow, treeBlockRows)

	//fmt.Println("gpos", pos)
	//fmt.Println("forsetRows", cow.manifest.forestRows)
	//fmt.Println("treeBlockRow", treeBlockRow)
	//fmt.Println("treeBlockOffset", treeBlockOffset)
	//fmt.Println("treeTableOffset", treeTableOffset)
	//fmt.Println("fetch", fetch)
	//fmt.Println(cow.manifest.location)
	//fmt.Println(cow.cachedTreeTables)

	hash := tb.leaves[fetch]

	if hash == empty {
		//fmt.Printf("READ RETURN with hash: %x\n", hash)
		//s := fmt.Sprintln("gpos", pos)
		//s += fmt.Sprintln("treeBlockOffset", treeBlockOffset)
		//s += fmt.Sprintln("treeBlockRow", treeBlockRow)
		//s += fmt.Sprintln("forestRows", cow.manifest.forestRows)
		//s += fmt.Sprintln("fetch", fetch)
		//fmt.Println(s)
		//fmt.Println(tb)
	}
	//fmt.Printf("READ RETURN on pos: %d with hash: %x\n",
	//	pos, hash)
	return hash
}

// write changes the in-memory representation of the relevant treeBlock
// NOTE The treeBlocks on disk are not changed. commit must be called for that
func (cow *cowForest) write(pos uint64, h Hash) {
	//fmt.Printf("WRITE CALLED on pos: %d with hash: %x\n", pos, h)

	if pos > getRowOffset(cow.manifest.forestRows, cow.manifest.forestRows) {
		s := fmt.Errorf("pos of %d is greater than the max of what forestRows"+
			"%d can hold\n", pos, cow.manifest.forestRows)
		panic(s)
	}

	treeBlockRow, treeBlockOffset, err := getTreeBlockPos(pos, cow.manifest.forestRows)
	if err != nil {
		//return Hash{}, err
		panic(err)
	}
	_, treeTableOffset := getTreeTablePos(pos, cow.manifest.forestRows)

	// grab the treeTable location. This is just a number for the .ufod file
	location := cow.manifest.location[treeBlockRow][treeTableOffset]

	// check if it exists in memory
	table, found := cow.cachedTreeTables[location]

	// if not found in memory, load then update the fileNum
	if !found {
		//fmt.Println("TABLE NOT FOUND IN MEMORY")
		err = cow.load(location)
		if err != nil {
			//return Hash{}, err
			panic(err)
		}
		table = cow.cachedTreeTables[location]

		// advance fileNum and set as new file
		cow.manifest.fileNum++
		cow.manifest.location[treeBlockRow][treeBlockOffset/treeBlockPerTable] =
			cow.manifest.fileNum

		// set as table
		cow.cachedTreeTables[cow.manifest.fileNum] = table

		// delete old key
		delete(cow.cachedTreeTables, location)

		// add file to be cleaned up
		cow.meta.staleFiles = append(
			cow.meta.staleFiles, location)
	}

	// there there is no treeBlock, then attach one
	if table.memTreeBlocks[treeBlockOffset%treeBlockPerTable] == nil {
		table.memTreeBlocks[treeBlockOffset%treeBlockPerTable] = new(treeBlock)
	}

	locRow, localPos := gPosToLocPos(
		pos, treeBlockOffset, treeBlockRow, cow.manifest.forestRows)
	//fmt.Println(locRow, localPos)

	fetch := localPos + getRowOffset(locRow, treeBlockRows)
	table.memTreeBlocks[treeBlockOffset%treeBlockPerTable].leaves[fetch] = h

	// sanity checking
	compH := cow.read(pos)
	if compH != h {
		fmt.Printf("%x\n", table.memTreeBlocks[treeBlockOffset%treeBlockPerTable].leaves[fetch])
		err := fmt.Errorf("the hash written doesn't equal what's supposed to be written"+
			"written %x but read %x\n", h, compH)
		panic(err)
	}
	//fmt.Println("WRITE RETURN")
	//fmt.Printf("%x\n", table.memTreeBlocks[0].leaves)
}

// swapHash takes in two hashes and atomically swaps them.
// NOTE The treeBlocks on disk are not changed. commit must be called for that
func (cow *cowForest) swapHash(a, b uint64) {
	//fmt.Println("SWAPHASH CALLED")

	aHash := cow.read(a)
	bHash := cow.read(b)

	cow.write(a, bHash)
	cow.write(b, aHash)

	//fmt.Println("SWAPHASH RETURN")
}

func (cow *cowForest) swapHashRange(a, b, w uint64) {
	//fmt.Println("SWAPHASHRANGE CALLED")
	/*
		var aBefore []Hash

		for i := a; i <= a+w; i++ {
			aBefore = append(aBefore, cow.read(i))
		}

		var bBefore []Hash
		for i := b; i <= b+w; i++ {
			bBefore = append(bBefore, cow.read(i))
		}
		fmt.Println("aBefore")
		fmt.Println(aBefore)
		fmt.Println("bBefore")
		fmt.Println(bBefore)

		// load the tables to memory
		rowA, offsetsA, posSameOffsetsA := cow.findAndLoadRange(a, w)
		rowB, offsetsB, posSameOffsetsB := cow.findAndLoadRange(b, w)

		// fetch the hashes
		aHashes := make([]Hash, 0, w+1) // +1 as to include a
		bHashes := make([]Hash, 0, w+1) // +1 as to include b
		for i, offsetA := range offsetsA {
			var table *treeTable
			for _, pos := range posSameOffsetsA[i] {
				loc := cow.manifest.location[rowA][offsetA/treeBlockPerTable]
				table, _ = cow.cachedTreeTables[loc]
				treeBlock := table.memTreeBlocks[offsetA%treeBlockPerTable]
				_, localPos := gPosToLocPos(pos, offsetA, rowA, cow.manifest.forestRows)
				aHashes = append(aHashes, treeBlock.leaves[localPos])
			}
		}

		for i, offsetB := range offsetsB {
			var table *treeTable
			for _, pos := range posSameOffsetsB[i] {
				loc := cow.manifest.location[rowB][offsetB/treeBlockPerTable]
				table, _ = cow.cachedTreeTables[loc]
				treeBlock := table.memTreeBlocks[offsetB%treeBlockPerTable]
				_, localPos := gPosToLocPos(pos, offsetB, rowB, cow.manifest.forestRows)
				bHashes = append(bHashes, treeBlock.leaves[localPos])
			}
		}

		// Apply the hashes
		var bCount int // for iterating through bHashes. Ugly TODO
		for i, offsetA := range offsetsA {
			var table *treeTable
			loc := cow.manifest.location[rowA][offsetA/treeBlockPerTable]
			table, _ = cow.cachedTreeTables[loc]

			// Write hashes
			for _, pos := range posSameOffsetsA[i] {
				_, localPos := gPosToLocPos(pos, offsetA, rowA, cow.manifest.forestRows)
				table.memTreeBlocks[offsetA%treeBlockPerTable].leaves[localPos] = bHashes[bCount]
				bCount++
			}
			//cow.updateTableNum(loc, offsetA, rowA)
		}

		var aCount int // for iterating through bHashes. Ugly TODO
		for i, offsetB := range offsetsB {
			var table *treeTable
			loc := cow.manifest.location[rowB][offsetB/treeBlockPerTable]
			table, _ = cow.cachedTreeTables[loc]

			// Write hashes
			for _, pos := range posSameOffsetsB[i] {
				_, localPos := gPosToLocPos(pos, offsetB, rowB, cow.manifest.forestRows)
				table.memTreeBlocks[offsetB%treeBlockPerTable].leaves[localPos] = aHashes[aCount]
				aCount++
			}
			//cow.updateTableNum(loc, offsetB, rowB)
		}

		// SANITY CHECKING DOWN HERE

		var counter int
		// Check all hashes
		var aAfter []Hash
		for i := a; i <= a+w; i++ {
			//if bBefore[counter] != cow.read(i) {
			//	panic("swaphashes written swaps don't match")
			//}
			aAfter = append(aAfter, cow.read(i))
			counter++
		}

		counter = 0
		var bAfter []Hash
		for i := b; i <= b+w; i++ {
			//if aBefore[counter] != cow.read(i) {
			//	panic("swaphashes written swaps don't match")
			//}
			bAfter = append(bAfter, cow.read(i))
			counter++
		}
		fmt.Println("COMPARE SWAPHASH")
		fmt.Println(a, w)
		fmt.Println(b, w)
		fmt.Println(aBefore)
		fmt.Println(aAfter)

		fmt.Println()
		fmt.Println(bBefore)
		fmt.Println(bAfter)
	*/
	aHashes := make([]Hash, 0, w+1) // +1 as to include a
	bHashes := make([]Hash, 0, w+1) // +1 as to include b
	for i := a; i < a+w; i++ {
		aHashes = append(aHashes, cow.read(i))
	}
	for i := b; i < b+w; i++ {
		bHashes = append(bHashes, cow.read(i))
	}

	var counter int
	for i := a; i < a+w; i++ {
		cow.write(i, bHashes[counter])
		counter++
	}

	counter = 0
	for i := b; i < b+w; i++ {
		cow.write(i, aHashes[counter])
		counter++
	}

	//fmt.Println("SWAPHASHRANGE RETURN")
}

func (cow *cowForest) size() uint64 {
	// grab the count of the treeTables for treeBlockRow0
	// Since each represents treeBlockPerTable amount of leaves, multiply
	// by that.

	// if location hasn't been initialized, return 0
	/*
		if len(cow.manifest.location) == 0 {
			return 0
		}
	*/

	// if it has been, return the length of treeBlockRow 0
	return uint64(2 << cow.manifest.forestRows)
}

// FIXME newSize is the size of the entire forest. I have it as the leafcount
func (cow *cowForest) resize(newSize uint64) {
	//fmt.Println("RESIZE CALLED")

	tbRows := cow.manifest.forestRows / rowPerTreeBlock
	//fmt.Println("treeBlockRows", treeBlockRows)

	if len(cow.manifest.location) <= int(tbRows) {
		cow.manifest.location = append(cow.manifest.location, []uint64{})
		cow.newTable(tbRows)
	}

	// append new treeTables as needed
	for row := uint8(0); row <= tbRows; row++ {
		//fmt.Println("len: ", len(cow.manifest.location[row]))
		currentCap := len(cow.manifest.location[row]) * leavesPerTreeTable
		// only add new tables if the current row can't hold what's needed
		for newSize > uint64(currentCap) {
			cow.newTable(row)
			currentCap += leavesPerTreeTable
		}

		// size for the next row
		// FIXME this does'nt work
		//newSize = newSize / uint64(2*treeBlockRows)
		newSize >>= 6
	}

	// update forestRows
	// TODO sorta ugly here because you could just pass in the forestRows
	// but you'd need to change the interface for that
	//cow.manifest.forestRows = treeRows(newSize)
	//cow.manifest.forestRows += 3

	//fmt.Println(cow.manifest.location)
	//fmt.Println(cow.cachedTreeTables)
	//fmt.Println("RESIZE RETURN")
}

func (cow *cowForest) setRow(row uint8) {
	//fmt.Println("SETROW TO:", row)
	cow.manifest.forestRows = row
}

func (cow *cowForest) close() {
	fmt.Println("COWFOREST CLOSE")
	// commit the current forest
	err := cow.commit()
	if err != nil {
		fmt.Printf("cowForest close error:\n%s\n"+
			"Previously saved forest not overwritten", err)
	}
}

// finds the file and loads it to memory. Does nothing it it's already been
// loaded.
func (cow *cowForest) findAndLoad(pos uint64) (uint8, uint64) {
	treeBlockRow, treeBlockOffset, err := getTreeBlockPos(
		pos, cow.manifest.forestRows)
	if err != nil {
		//return Hash{}, err
		panic(err)
	}

	// grab the treeTable location. This is just a number for the .ufod file
	location := cow.manifest.location[treeBlockRow][treeBlockOffset/treeBlockPerTable]

	// check if it exists in memory
	_, found := cow.cachedTreeTables[location]

	// Table is not in memory
	if !found {
		// Load the treeTable onto memory. This maps the table to the location
		err = cow.load(location)
		if err != nil {
			//return Hash{}, err
			panic(err)
		}
		//table = cow.cachedTreeTables[location]
	}
	return treeBlockRow, treeBlockOffset
}

func (cow *cowForest) findAndLoadRange(pos, width uint64) (
	row uint8, treeBlockOffsets []uint64, posSameOffsets [][]uint64) {
	// check if the positions are at different treeBlocks
	posSameOffsets, treeBlockOffsets, row = getTreeBlockPosRange(pos, width, cow.manifest.forestRows)

	for offset := range treeBlockOffsets {
		location := cow.manifest.location[row][offset/treeBlockPerTable]
		// check if it exists in memory
		_, found := cow.cachedTreeTables[location]

		// Table is not in memory
		if !found {
			// Load the treeTable onto memory. This maps the table to the location
			err := cow.load(location)
			if err != nil {
				//return Hash{}, err
				panic(err)
			}
			//table = cow.cachedTreeTables[location]
		}

	}
	return
}

// Adds a single new table to the given treeBlockRow in memory
func (cow *cowForest) newTable(treeBlockRow uint8) {
	if len(cow.manifest.location) <= int(treeBlockRow) {
		cow.manifest.location = append(cow.manifest.location, []uint64{})
	}
	cow.manifest.fileNum++
	cow.manifest.location[treeBlockRow] = append(
		cow.manifest.location[treeBlockRow], cow.manifest.fileNum)

	newTable := newTreeTable()

	cow.cachedTreeTables[cow.manifest.fileNum] = newTable
}

// Update the cowForest num given table location. Returns the new location
func (cow *cowForest) updateTableNum(
	location, treeBlockOffset uint64, treeBlockRow uint8) uint64 {
	//fmt.Println("UPDATETABLE NUM CALLED")

	// grab the table
	table := cow.cachedTreeTables[location]
	if table == nil {
		panic("fetched table is nil")
	}
	tableNew := *table

	// advance fileNum and set as new file
	cow.manifest.fileNum++
	cow.manifest.location[treeBlockRow][treeBlockOffset/treeBlockPerTable] =
		cow.manifest.fileNum

	// set as table
	cow.cachedTreeTables[cow.manifest.fileNum] = &tableNew

	// delete old key
	delete(cow.cachedTreeTables, location)

	t := cow.cachedTreeTables[cow.manifest.fileNum]
	if t == nil {
		panic("fetched t is nil")
	}

	return cow.manifest.fileNum
}

// Load will load the existing forest from the disk given a fileNumber
func (cow *cowForest) load(fileNum uint64) error {
	//fmt.Println("LOAD IS CALLED")
	stringLoc := strconv.FormatUint(fileNum, 10) // base 10 used
	filePath := filepath.Join(cow.meta.fBasePath, stringLoc+extension)

	f, err := os.Open(filePath)
	if err != nil {
		// If the error returned is of no files existing, then the manifest
		// is corrupt
		if os.IsNotExist(err) {
			// Not sure if we can recover from this? I think panic
			// is the right call
			panic(errorCorruptManifest())
			//return errorCorruptManifest()

		}
		return err
	}
	// treeBlockPerTable treeBlocks in table, 127 leaves + 32 byte meta in a treeBlock
	// 32 bytes per leaf
	buf := bufio.NewReaderSize(f, treeBlockPerTable*127*32)

	//var leaves [127]Hash
	var leaves [4064]byte
	//var leaves []byte
	//var meta [32]byte
	newTable := new(treeTable)

	uint16Buf := make([]byte, 2)
	// FIXME read the length first
	treeBlockCount, err := buf.Read(uint16Buf)
	if err != nil {
		return err
	}

	// treeBlockPerTable treeBlocks in a treeTable
	// FIXME Some treeTables may have less than 1024 treeBlocks
	for i := 0; i < treeBlockCount; i++ {
		tb := new(treeBlock)
		//buf.Read(meta[:])
		buf.Read(leaves[:])

		for j := 0; j < 127; j++ {
			//fmt.Println(tb.leaves[j])
			//fmt.Println(len(tb.leaves[j]))
			offset := j * 32
			copy(tb.leaves[j][:], leaves[offset:offset+31])
		}

		newTable.memTreeBlocks[i] = tb

	}
	// set map
	cow.cachedTreeTables[fileNum] = newTable

	fmt.Println(cow.cachedTreeTables)
	return nil
}

// commit makes writes to the disk and sets the forest to point to the new
// treeBlocks. The new forest state is commited to disk only when commit is called
func (cow *cowForest) commit() error {
	err := cow.manifest.commit(cow.meta.fBasePath)
	if err != nil {
		// maybe if it couldn't commit then it should panic?
		return err
	}

	for fileNum, treeTable := range cow.cachedTreeTables {
		// calculate the file name
		stringLoc := strconv.FormatUint(fileNum, 10) // base 10 used
		fPath := filepath.Join(cow.meta.fBasePath, stringLoc)
		fName := fPath + extension

		f, err := os.OpenFile(fName, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return err
		}

		length, serializedTable := treeTable.serialize()

		lenBytes := make([]byte, 2)
		binary.LittleEndian.PutUint16(lenBytes[:], length)

		// write to that file
		_, err = f.Write(lenBytes)
		if err != nil {
			return err
		}
		_, err = f.Write(serializedTable)
		if err != nil {
			return err
		}
	}
	// clean up all old stale files
	cow.clean()

	return nil
}

// Clean removes all the stale treeTables from the disk
func (cow *cowForest) clean() error {
	for _, fileNum := range cow.meta.staleFiles {
		stringLoc := strconv.FormatUint(fileNum, 10) // base 10 used
		filePath := filepath.Join(cow.meta.fBasePath, stringLoc)
		err := os.Remove(filePath)
		if err != nil {
			return err
		}
	}
	return nil
}

type diskForestData struct {
	file *os.File
}

// read ignores errors. Probably get an empty hash if it doesn't work
func (d *diskForestData) read(pos uint64) Hash {
	var h Hash
	_, err := d.file.ReadAt(h[:], int64(pos*leafSize))
	if err != nil {
		fmt.Printf("\tWARNING!! read %x pos %d %s\n", h, pos, err.Error())
	}
	return h
}

// writeHash writes a hash.  Don't go out of bounds.
func (d *diskForestData) write(pos uint64, h Hash) {
	_, err := d.file.WriteAt(h[:], int64(pos*leafSize))
	if err != nil {
		fmt.Printf("\tWARNING!! write pos %d %s\n", pos, err.Error())
	}
}

// swapHash swaps 2 hashes.  Don't go out of bounds.
func (d *diskForestData) swapHash(a, b uint64) {
	ha := d.read(a)
	hb := d.read(b)
	d.write(a, hb)
	d.write(b, ha)
}

// swapHashRange swaps 2 continuous ranges of hashes.  Don't go out of bounds.
// uses lots of ram to make only 3 disk seeks (depending on how you count? 4?)
// seek to a start, read a, seek to b start, read b, write b, seek to a, write a
// depends if you count seeking from b-end to b-start as a seek. or if you have
// like read & replace as one operation or something.
func (d *diskForestData) swapHashRange(a, b, w uint64) {
	arange := make([]byte, leafSize*w)
	brange := make([]byte, leafSize*w)
	_, err := d.file.ReadAt(arange, int64(a*leafSize)) // read at a
	if err != nil {
		fmt.Printf("\tshr WARNING!! read pos %d len %d %s\n",
			a*leafSize, w, err.Error())
	}
	_, err = d.file.ReadAt(brange, int64(b*leafSize)) // read at b
	if err != nil {
		fmt.Printf("\tshr WARNING!! read pos %d len %d %s\n",
			b*leafSize, w, err.Error())
	}
	_, err = d.file.WriteAt(arange, int64(b*leafSize)) // write arange to b
	if err != nil {
		fmt.Printf("\tshr WARNING!! write pos %d len %d %s\n",
			b*leafSize, w, err.Error())
	}
	_, err = d.file.WriteAt(brange, int64(a*leafSize)) // write brange to a
	if err != nil {
		fmt.Printf("\tshr WARNING!! write pos %d len %d %s\n",
			a*leafSize, w, err.Error())
	}
}

// size gives you the size of the forest
func (d *diskForestData) size() uint64 {
	s, err := d.file.Stat()
	if err != nil {
		fmt.Printf("\tWARNING: %s. Returning 0", err.Error())
		return 0
	}
	return uint64(s.Size() / leafSize)
}

// resize makes the forest bigger (never gets smaller so don't try)
func (d *diskForestData) resize(newSize uint64) {
	err := d.file.Truncate(int64(newSize * leafSize * 2))
	if err != nil {
		panic(err)
	}
}

func (d *diskForestData) close() {
	fmt.Println("DISKFOREST CLOSE")
	err := d.file.Close()
	if err != nil {
		fmt.Printf("diskForestData close error: %s\n", err.Error())
	}
}
func (d *diskForestData) setRow(row uint8) {
}
