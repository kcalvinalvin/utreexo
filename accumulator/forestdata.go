package accumulator

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
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

// ********************************************* forest on disk

// cowForestConfig is for configurations for a cowForest
type cowForestConfig struct {
	// rowPerTreeBlock is the rows a treeBlock holds
	rowPerTreeBlock uint8
}

// rowPerTreeBlock is the rows a treeBlock holds
// 7 is chosen as a tree with height 6 will contain 7 rows (row 0 to row 6)
// TODO duplicate of Config
//const rowPerTreeBlock = 7
const rowPerTreeBlock = 3

// This is the same concept as forestRows, except for treeBlocks.
// Fixed as a treeBlockRow cannot increase. You just make another treeBlock if the
// current one isn't enough.
//const treeBlockRows = 6
const treeBlockRows = 2

// Number for the amount of treeBlocks to go into a table
//const treeBlockPerTable = 1024
const treeBlockPerTable = 32

// Number of leaves that a treeBlock holds
const leavesPerTreeBlock = 1 << treeBlockRows

// Number of leaves that a treeTable holds
const leavesPerTreeTable = leavesPerTreeBlock * treeBlockPerTable

// extension for the forest files on disk. Stands for, "Utreexo Forest
// On Disk
var extension string = ".ufod"

type TreeBlockState int

const (
	// current is if the treeBlock is part of the latest Utreexo forest
	// state. Necessary as utreexo forest is a copy-on-write and doesn't
	// delete the old state
	current TreeBlockState = iota

	// sparse marks the current treeBlock as having empty nodes
	sparse

	// bottom indicates that this treeBlock contains the UTXO hashes
	bottom
)

var (
	ErrorCorruptManifest = errors.New("Manifest is corrupted. Recovery needed")
)

func errorCorruptManifest() error { return ErrorCorruptManifest }

// manifest is the structure saved on disk for loading the current
// utreexo forest
// FIXME fix padding
type manifest struct {
	// The number following 'MANIFEST'
	// A new one will be written on every db open
	currentManifestNum uint64

	// The current .ufod file number. This is incremented on every
	// new treeTable
	fileNum int64

	// The latest synced Bitcoin block hash
	currentBlockHash Hash

	// The latest synced Bitcoin block height
	currentBlockHeight int32

	// The current allocated rows in forest
	forestRows uint8

	// location holds the on-disk fileNum for the treeTables. 1st array
	// holds the treeBlockRow info and the seoncd holds the offset
	location [][]int64

	// staleFiles are the files that are not part of the latest forest state
	// these should be cleaned up.
	staleFiles []int64
}

// commit creates a new manifest version and commits it and removes the old manifest
// The commit is atomic in that only when the commit was successful, the
// old manifest is removed.
func (m *manifest) commit() error {
	manifestNum := m.currentManifestNum + 1

	fName := fmt.Sprintf("MANIFEST-%d", manifestNum)

	fNewManifest, err := os.OpenFile(fName, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		return err
	}

	buf := make([]byte, 37)
	binary.LittleEndian.PutUint32(buf, uint32(m.currentBlockHeight))
	buf = append(buf, m.currentBlockHash[:]...)
	buf = append(buf, byte(m.forestRows))

	_, err = fNewManifest.Write(buf)
	if err != nil {
		return err
	}

	// Overwrite the current manifest number in CURRENT
	fCurrent, err := os.OpenFile("CURRENT", os.O_WRONLY, 0655)
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
	err = os.Remove(fOldName)
	if err != nil {
		// ErrOldManifestNotRemoved
		return err
	}

	return nil
}

// load loades the manifest from the disk
func (m *manifest) load() error {
	f, err := os.OpenFile("CURRENT", os.O_RDONLY, 0444)
	if err != nil {
		return err
	}
	buf := make([]byte, 37)
	_, err = f.Read(buf)
	if err != nil {
		return err
	}

	m.currentBlockHeight = int32(binary.LittleEndian.Uint32(buf[:3]))
	copy(m.currentBlockHash[:], buf[4:36])
	m.forestRows = uint8(buf[37])

	return nil
}

// getTreeBlockPos grabs the relevant treeBlock position.
func getTreeBlockPos(pos uint64, forestRows uint8) (
	treeBlockRow uint8, treeBlockOffset uint64, err error) {

	// For testing. These should be set within the cowforest
	rowPerTreeBlock := uint8(3)
	TreeBlockRows := uint8(2)

	// maxPossiblePosition is the upper limit for the position that a given tree
	// can hold. Should error out if a requested position is greater than it.
	maxPossiblePosition := getRowOffset(forestRows, forestRows)
	if pos > maxPossiblePosition {
		err = fmt.Errorf("Position requested is more than the forest can hold")
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
	fmt.Println("localRowPos", localRowPos)

	leafCount := 1 << forestRows       // total leaves in the forest
	nodeCountAtRow := leafCount >> row // total nodes in this row
	//treeBlockCountAtRow := nodeCountAtRow / (1 << 2) // change 2 to the relevant forestRows
	nodeCountAtTreeBlockRow := leafCount >> (treeBlockRow * rowPerTreeBlock) // total nodes in this TreeBlockrow
	fmt.Println("nodeCountAtTreeBlockRow:", nodeCountAtTreeBlockRow)

	// If there are less nodes at row than a treeBlock holds, it means that there
	// are empty leaves in that single treeBlock
	var treeBlockCountAtRow int
	if nodeCountAtTreeBlockRow < (1 << TreeBlockRows) {
		// Only 1 treeBlock, the top treeBlock, may be sparse.
		treeBlockCountAtRow = 1
	} else {
		//treeBlockCountAtRow = nodeCountAtRow / (1 << TreeBlockRows)
		treeBlockCountAtRow = nodeCountAtTreeBlockRow / (1 << TreeBlockRows)
	}
	fmt.Println("treeBlockCountAtRow:", treeBlockCountAtRow)

	// In a given row, how many leaves go into a treeBlock of that row?
	// For exmaple, a forest with:
	// row = 1, rowPerTreeBlock = 3
	// maxLeafPerTreeBlockAtRow = 2
	//
	// another would be
	// row = 0, rowPerTreeBlock = 3
	// maxLeafPerTreeBlockAtRow = 4
	maxLeafPerTreeBlockAtRow := nodeCountAtRow / treeBlockCountAtRow
	fmt.Println("maxLeafPerTreeBlockAtRow", maxLeafPerTreeBlockAtRow)

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

// Translate a global position to its local position.
func gPosToLocPos(gPos, offset uint64, treeBlockRow, forestRows uint8) (
	uint8, uint64) {

	rowPerTreeBlock := uint8(3)
	//treeBlockRows := uint8(2)

	fmt.Println()
	fmt.Println()
	fmt.Println()
	fmt.Println()
	fmt.Println()

	row := detectRow(gPos, forestRows)
	rOffset := getRowOffset(row, forestRows)

	// the position relevant to the row
	rowPos := gPos - rOffset
	fmt.Println("rowPos", rowPos)

	// total leaves in the forest
	leafCount := 1 << forestRows

	// total nodes in this row
	nodeCountAtRow := leafCount >> row
	fmt.Println("nodeCountAtRow", nodeCountAtRow)

	// total nodes in this treeBlockRow
	nodeCountAtTreeBlockRow := leafCount >> (treeBlockRow * rowPerTreeBlock)
	fmt.Println("nodeCountAtTreeBlockRow", nodeCountAtTreeBlockRow)

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
	fmt.Println("treeBlockCountAtRow", treeBlockCountAtRow)

	rowBlockOffset := offset * uint64(nodeCountAtRow/treeBlockCountAtRow)
	locPos := rowPos - rowBlockOffset
	locRow := (row - (rowPerTreeBlock * treeBlockRow))

	return locRow, locPos

}

/*
// getMin grabs the minimum leaf position a TreeTable can hold
func getMin(max, blockRow uint64, maxCachedTables int) uint64 {
	stored := uint64(maxCachedTables * treeBlockPerTable)
	leafRange := uint64(stored * blockRow)
	return max - leafRange
}

func getRange(blockRow uint64) uint64 {
	return uint64(treeBlockPerTable * blockRow)
}
*/

// treeBlockLeaf is a leaf in any given treeBlock and represents the lowest
// nodes in a given tree
type treeBlockLeaf struct {
	data      Hash
	treeBlock *treeBlock
}

// treeBlock is a representation of a row 6 utreexo tree.
// It contains 127 leaves and 32 bytes of metadata
// The leaves can be a UTXO hash or another treeBlock
type treeBlock struct {
	// TODO Lots of things we can do with 32 bytes
	//meta [32]byte

	// row is the row number for the heighest row in this treeBlock
	// row can only have a value that is a multiple of six, as treeBlocks
	// are organized into six-row trees
	//
	// Ex:
	// The bottommost treeBlocks in the entire Utreexo tree will have a
	// row value of 6.
	row uint8

	// meta are the bitflags to represent the state of the treeBlock
	meta TreeBlockState

	// leaves are the utreexo nodes that are either
	// 1: Hash of a UTXO
	// 2: Parent hash of two nodes
	//
	// Able to hold up to 64 leaves
	leaves [127]Hash
}

// converts a treeBlock to byte slice
func (tb *treeBlock) toBytes() []byte {
	buf := make([]byte, 127*32)
	for _, leaf := range tb.leaves {
		buf = append(buf, leaf[:]...)
	}
	return buf
}

type treeTableState int

const (
	inMemory treeTableState = iota
)

// treeTable is a group of treeBlocks that are sorted on disk
// A treeTable only contains the treeBlocks that are of the same row
// The included treeBlocks are sorted then stored onto disk
type treeTable struct {
	// bitflags for the treeTable state
	state treeTableState

	treeBlockRow uint8
	// memTreeBlocks is the treeBlocks that are stored in memory before they are
	// written to disk. This is helpful as older treeBlocks get less and
	// less likely to be accessed as stated in 5.7 of the utreexo paper
	// NOTE 1024 is the current value of stored treeBlocks per treeTable
	// this value may change/can be changed
	memTreeBlocks [treeBlockPerTable]*treeBlock
}

func (tt *treeTable) toBytes() []byte {
	// 32 is the sha256 hash
	buf := make([]byte, 0, leavesPerTreeTable*32)
	for _, tb := range tt.memTreeBlocks {
		buf = append(buf, tb.toBytes()...)
	}
	return buf
}

// Shorthand for copy-on-write. Unfortuntely, it doesn't go moo
type cowForest struct {
	// cachedTreeTables are the in-memory tables that are not yet committed to disk
	// TODO flush these after a certain number is in memory
	cachedTreeTables map[int64]*treeTable

	// manifest contains all the necessary metadata for fetching
	// utreexo nodes
	manifest manifest

	// fBasePath is the base directory for the .ufod files
	fBasePath string
}

// initalize returns a cowForest with a maxCachedTables value set
func initialize(path string) (*cowForest, error) {
	cow := cowForest{
		fBasePath: path,
	}
	cow.cachedTreeTables = make(map[int64]*treeTable)
	return &cow, nil
}

// Read takes a position and forestRows to return the Hash of that leaf
func (cow *cowForest) read(pos uint64) Hash {
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

	// grab the treeTable location. This is just a number for the .ufod file
	location := cow.manifest.location[treeBlockRow][treeBlockOffset/treeBlockPerTable]

	// check if it exists in memory
	table, found := cow.cachedTreeTables[location]

	// Table is not in memory
	if !found {
		// Load the treeTable onto memory. This maps the table to the location
		err = cow.load(location)
		if err != nil {
			//return Hash{}, err
			panic(err)
		}
		table = cow.cachedTreeTables[location]
	}

	treeBlock := table.memTreeBlocks[treeBlockOffset%treeBlockPerTable]

	_, localPos := gPosToLocPos(pos, treeBlockOffset, treeBlockRow, cow.manifest.forestRows)

	return treeBlock.leaves[localPos]
}

// write changes the in-memory representation of the relevant treeBlock
// NOTE The treeBlocks on disk are not changed. commit must be called for that
func (cow *cowForest) write(pos uint64, h Hash) {
	treeBlockRow, treeBlockOffset, err := getTreeBlockPos(pos, cow.manifest.forestRows)
	if err != nil {
		//return Hash{}, err
		panic(err)
	}

	// grab the treeTable location. This is just a number for the .ufod file
	location := cow.manifest.location[treeBlockRow][treeBlockOffset/treeBlockPerTable]

	// check if it exists in memory
	table, found := cow.cachedTreeTables[location]

	// if not found in memory, load then update the fileNum
	if !found {
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
		cow.manifest.staleFiles = append(
			cow.manifest.staleFiles, location)
	}

	_, localPos := gPosToLocPos(
		pos, treeBlockOffset, treeBlockRow, cow.manifest.forestRows)

	table.memTreeBlocks[treeBlockOffset%treeBlockPerTable].leaves[localPos] = h
}

// swapHash takes in two hashes and atomically swaps them.
// NOTE The treeBlocks on disk are not changed. commit must be called for that
func (cow *cowForest) swapHash(a, b uint64) {
	// Load tables to memory
	treeBlockRowA, treeBlockOffsetA := cow.findAndLoad(a)
	treeBlockRowB, treeBlockOffsetB := cow.findAndLoad(b)

	// grab the treeTable location. This is just a number for the .ufod file
	locationA := cow.manifest.location[treeBlockRowA][treeBlockOffsetA/treeBlockPerTable]
	locationB := cow.manifest.location[treeBlockRowB][treeBlockOffsetB/treeBlockPerTable]

	tableA, _ := cow.cachedTreeTables[locationA]
	tableB, _ := cow.cachedTreeTables[locationB]

	treeBlockA := tableA.memTreeBlocks[treeBlockOffsetA%treeBlockPerTable]
	treeBlockB := tableB.memTreeBlocks[treeBlockOffsetB%treeBlockPerTable]

	_, localPosA := gPosToLocPos(a, treeBlockOffsetA, treeBlockRowA, cow.manifest.forestRows)
	_, localPosB := gPosToLocPos(b, treeBlockOffsetB, treeBlockRowB, cow.manifest.forestRows)

	// fetch
	hashA := treeBlockA.leaves[localPosA]
	hashB := treeBlockB.leaves[localPosB]

	// set hashes
	tableA.memTreeBlocks[treeBlockOffsetA%treeBlockPerTable].leaves[localPosA] = hashB
	tableB.memTreeBlocks[treeBlockOffsetB%treeBlockPerTable].leaves[localPosB] = hashA

	// update the table number
	cow.updateTableNum(locationA, treeBlockOffsetA, treeBlockRowA)
	cow.updateTableNum(locationB, treeBlockOffsetB, treeBlockRowB)
}

func (cow *cowForest) swapHashRange(a, b, w uint64) {
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

	// Apply the hashes and update table .ufod number
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
		cow.updateTableNum(loc, offsetA, rowA)
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
		cow.updateTableNum(loc, offsetB, rowB)
	}
}

func (cow *cowForest) size() uint64 {
	// grab the count of the treeTables for treeBlockRow0
	// Since each represents treeBlockPerTable amount of leaves, multiply
	// by that.
	return uint64(len(cow.manifest.location[0]) * treeBlockPerTable)
}

func (cow *cowForest) resize(newSize uint64) {
	// get the table count for bottom row
	bRowSize := cow.size()

	// If what we currently have is bigger or euqal, no need to do anything
	if uint64(bRowSize) > newSize {
		return
	}

	// append new treeTables as needed
	for i := uint8(0); i <= cow.manifest.forestRows; i++ {
		leafCountAtRow := 1 << i

		// only add new tables if the current row can't hold what's needed
		for newSize > uint64(leafCountAtRow) {
			cow.newTable(i)
			leafCountAtRow += treeBlockPerTable
		}

		// size for the next row
		newSize = newSize / 2
	}
}

func (cow *cowForest) close() {
	// commit the current forest
	err := cow.commit()
	if err != nil {
		fmt.Printf("cowForest close error: %s "+
			"Previously saved forest not overwritten", err)
	}
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
	cow.manifest.fileNum++
	cow.manifest.location[treeBlockRow] = append(
		cow.manifest.location[treeBlockRow], cow.manifest.fileNum)

	newTable := new(treeTable)

	cow.cachedTreeTables[cow.manifest.fileNum] = newTable
}

// Update the cowForest num given table location. Returns the new location
func (cow *cowForest) updateTableNum(
	location int64, treeBlockOffset uint64, treeBlockRow uint8) int64 {

	// grab the table
	table := cow.cachedTreeTables[location]

	// advance fileNum and set as new file
	cow.manifest.fileNum++
	cow.manifest.location[treeBlockRow][treeBlockOffset/treeBlockPerTable] =
		cow.manifest.fileNum

	// set as table
	cow.cachedTreeTables[cow.manifest.fileNum] = table

	// delete old key
	delete(cow.cachedTreeTables, location)

	return cow.manifest.fileNum
}

// Load will load the existing forest from the disk given a fileNumber
func (cow *cowForest) load(fileNum int64) error {
	stringLoc := strconv.FormatInt(fileNum, 10) // base 10 used
	filePath := filepath.Join(cow.fBasePath, stringLoc)

	f, err := os.Open(filePath + extension)
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
	buf := bufio.NewReaderSize(f, treeBlockPerTable*128*32)

	//var leaves [127]Hash
	var leaves [4064]byte
	var meta [32]byte
	newTable := new(treeTable)
	// treeBlockPerTable treeBlocks in a treeTable
	for i := 0; i < treeBlockPerTable; i++ {
		buf.Read(meta[:])
		buf.Read(leaves[:])
		for j := 0; j < 32; j++ {
			offset := j * 32
			copy(newTable.memTreeBlocks[i].leaves[j][:],
				leaves[offset:offset+31])
		}
	}
	// set map
	cow.cachedTreeTables[fileNum] = newTable

	return nil
}

// commit makes writes to the disk and sets the forest to point to the new
// treeBlocks. The new forest state is commited to disk only when commit is called
func (cow *cowForest) commit() error {
	err := cow.manifest.commit()
	if err != nil {
		// maybe if it couldn't commit then it should panic?
		return err
	}

	for fileNum, treeTable := range cow.cachedTreeTables {
		// calculate the file name
		stringLoc := strconv.FormatInt(fileNum, 10) // base 10 used
		fPath := filepath.Join(cow.fBasePath, stringLoc)
		fName := fPath + extension

		f, err := os.OpenFile(fName, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return err
		}

		// write to that file
		_, err = f.Write(treeTable.toBytes())
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
	for _, fileNum := range cow.manifest.staleFiles {
		stringLoc := strconv.FormatInt(fileNum, 10) // base 10 used
		filePath := filepath.Join(cow.fBasePath, stringLoc)
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
	err := d.file.Close()
	if err != nil {
		fmt.Printf("diskForestData close error: %s\n", err.Error())
	}
}
