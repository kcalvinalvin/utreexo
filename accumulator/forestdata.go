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
	read(pos uint64) Hash
	write(pos uint64, h Hash)
	swapHash(a, b uint64)
	swapHashRange(a, b, w uint64)
	size() uint64
	resize(newSize uint64) // make it have a new size (bigger)
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

// Config is for configurations for a cowForest
type Config struct {
	// rowPerTreeBlock is the rows a treeBlock holds
	rowPerTreeBlock uint8
}

// rowPerTreeBlock is the rows a treeBlock holds
// 7 is chosen as a tree with height 6 will contain 7 rows (row 0 to row 6)
// TODO duplicate of Config
const rowPerTreeBlock = 7

// This is the same concept as forestRows, except for treeBlocks.
// Fixed as a treeBlockRow cannot increase. You just make another treeBlock if the
// current one isn't enough.
const treeBlockRows = 6

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

	// location holds the on-disk fileNum for the treeTables
	location [][]int64

	// staleFiles are the files that are not part of the latest forest state
	// these should be cleaned up.
	staleFiles []int64
}

// save creates a new manifest version and saves it and removes the old manifest
// The save is atomic in that only when the save was successful, the
// old manifest is removed.
func (m *manifest) save() error {
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

/*
// getPosition, given the bottom position of a leaf and the forestRows, returns
// what depth the treeBlock is and the position of the leaf within the treeBlocks.
// posInTreeBlock starts from the bottom and goes to the top. Will be nil if
// forestRows is less than or equal to 6
// Ex: For a forestRows = 14
// posInTreeBlock = [posForSixTree, posForTwelveTree]
func getPosition(globalPos uint64, forestRows uint8) (
	treeBlockDepth uint8, posInTreeBlock []uint64, err error) {

	totalLeaves := uint64(1 << forestRows)

	for totalLeaves > 64 {
		totalLeaves /= 64
		treeBlockDepth++
	}

	// if the leftover totalLeaves is greater than 0, it means there is a
	// tree in memetable that holds treeBlocks
	if totalLeaves > 0 {
		posInTreeBlock = append(posInTreeBlock, globalPos)
	}

	for i := uint8(1); i <= treeBlockDepth; i++ {
		posInTreeBlock = append(
			posInTreeBlock, uint64(getPosInTreeBlock(globalPos, forestRows, i*6)))
	}

	return
}
*/

/*
// getTreeTable grabs the relevant treeBlock in the cached treeTable
// for a given position
func getTreeTable(pos, maxRange, treeBlocks uint64) uint64 {
	rangePerBlock := uint64(treeBlocks * 64)

	maxRange -= treeBlocks
	for pos > maxRange {
		maxRange -= treeBlocks
		num--
	}

	return
}
*/

// getTreeBlockPos grabs the relevant treeBlock position.
func getTreeBlockPos(pos uint64, forestRows uint8, maxCachedTables int) (
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
	stored := uint64(maxCachedTables * 1024)
	leafRange := uint64(stored * blockRow)
	return max - leafRange
}

func getRange(blockRow uint64) uint64 {
	return uint64(1024 * blockRow)
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

	// maxLeafRange is the max global leaf position this treeBlock can hold
	// treeBlocks in different rows can have the same maxLeafRange value
	// Ex: for a 0th six-tree, maxLeafRange = 63, as a 0th six-tree holds
	// leaf positions 0-63
	maxLeafRange uint64

	// meta are the bitflags to represent the state of the treeBlock
	meta TreeBlockState

	// leaves are the utreexo nodes that are either
	// 1: Hash of a UTXO
	// 2: Parent hash of two nodes
	//
	// Able to hold up to 64 leaves
	leaves [127]Hash
}

/*
// read takes a global leaf position and returns the Hash
func (tb *treeBlock) read(pos, blockNum uint64, forestRows uint8) (Hash, error) {
	row := detectRow(pos, forestRows)
	if row > tb.row {
		return Hash{}, fmt.Errorf("requested row greater than this treeBlock contains")
	}

	return Hash{}, nil
}
*/

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
	memTreeBlocks [1024]treeBlock
}

// Shorthand for copy-on-write. Unfortuntely, it doesn't go moo
type cowForest struct {
	// cachedTreeTables are cached when Read on cowForest is called
	// TODO empty these after a certain block or something
	cachedTreeTables map[int64]treeTable

	// pendingTreeTables are in-memory treeTables that are not filled/committed
	// and writen to disk yet.
	// row denotes the treeTable rows and the column denotes the maxLeafRange
	pendingTreeTables [][]treeTable

	// same as rows in Forest. Here to fit the interface. TODO Would
	// be better if wasn't here
	rows uint8

	// maxCachedTables denotes how many tables to hold before writing to
	// disk
	maxCachedTables int

	// currentMaxLeaf holds which leafRange for a treeTable. One value is
	// stored per treeTable. The highest value is used when there are multiple
	// cached tables
	currentMaxLeaf []uint64

	// manifest contains all the necessary metadata for fetching
	// utreexo nodes
	manifest manifest

	// fBasePath is the base directory for the .ufod files
	fBasePath string
}

// initalize returns a cowForest with a maxCachedTables value set
func initalize(maxCachedTables int) (*cowForest, error) {
	// Cache at least 1
	if maxCachedTables <= 0 {
		maxCachedTables = 1
	}
	cow := cowForest{
		maxCachedTables: maxCachedTables,
		fBasePath:       "~/.forest",
	}
	cow.cachedTreeTables = make(map[int64]treeTable)
	//cow.memTreeTables = make([][]treeTable, 1)
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

	treeBlockRow, treeBlockOffset, err := getTreeBlockPos(
		pos, cow.rows, cow.maxCachedTables)
	if err != nil {
		//return Hash{}, err
		panic(err)
	}

	// grab the treeTable location. This is just a number for the .ufod file
	location := cow.manifest.location[treeBlockRow][treeBlockOffset/1024]

	// check if it exists in memory
	table, found := cow.cachedTreeTables[location]

	// Table is in memory
	if !found {
		// Load the treeTable onto memory. This maps the table to the location
		err = cow.load(location)
		if err != nil {
			//return Hash{}, err
			panic(err)
		}
		table = cow.cachedTreeTables[location]
	}

	treeBlock := table.memTreeBlocks[treeBlockOffset%1024]

	_, localPos := gPosToLocPos(pos, treeBlockOffset, treeBlockRow, cow.rows)

	return treeBlock.leaves[localPos]
}

// write changes the in-memory representation of the relevant treeBlock
// NOTE The treeBlocks on disk are not changed. commit must be called for that
func (cow *cowForest) write(pos uint64, h Hash) {
	treeBlockRow, treeBlockOffset, err := getTreeBlockPos(
		pos, cow.rows, cow.maxCachedTables)
	if err != nil {
		//return Hash{}, err
		panic(err)
	}

	// grab the treeTable location. This is just a number for the .ufod file
	location := cow.manifest.location[treeBlockRow][treeBlockOffset/1024]

	// check if it exists in memory
	table, found := cow.cachedTreeTables[location]

	// if not found in memory, load then update the fileNum
	if !found {
		//delete(cow.cachedTreeTables, location)
		err = cow.load(location)
		if err != nil {
			//return Hash{}, err
			panic(err)
		}
		// set as table
		table = cow.cachedTreeTables[location]

		// advance fileNum and set as new file
		cow.manifest.fileNum++
		cow.manifest.location[treeBlockRow][treeBlockOffset/1024] =
			cow.manifest.fileNum

		// add file to be cleaned up
		cow.manifest.staleFiles = append(
			cow.manifest.staleFiles, location)
	}

	_, localPos := gPosToLocPos(pos, treeBlockOffset, treeBlockRow, cow.rows)
	table.memTreeBlocks[treeBlockOffset%1024].leaves[localPos] = h
}

// swapHash takes in two hashes and atomically swaps them.
// NOTE The treeBlocks on disk are not changed. Commit must be called for that
func (cow *cowForest) swapHash(a, b uint64) {
}

func (cow *cowForest) swapHashRange(a, b, w uint64) {
}

func (cow *cowForest) size() uint64 {
	return uint64(0)
}

func (cow *cowForest) resize(newSize uint64) {
	// nothing to do
}

func (cow *cowForest) close() {
	// commit the current forest
	err := cow.commit()
	if err != nil {
		fmt.Printf("cowForest save error: %s "+
			"Reverted back to the previously saved forest", err)
	}
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
	// 1024 treeBlocks in table, 127 leaves + 32 byte meta in a treeBlock
	// 32 bytes per leaf
	buf := bufio.NewReaderSize(f, 1024*128*32)

	//var leaves [127]Hash
	var leaves [4064]byte
	var meta [32]byte
	var newTable treeTable
	// 1024 treeBlocks in a treeTable
	for i := 0; i < 1024; i++ {
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

// GetSnapshot returns a snapshot of the forest in a given height and blockHash
func (cow *cowForest) GetSnapshot(blockHeight int32, hash Hash) *cowForest {
	// TODO placeholder
	return &cowForest{}
}

// Commit makes writes to the disk and sets the forest to point to the new
// treeBlocks. The new forest state is saved to disk only when Commit is called
func (cow *cowForest) commit() error {
	return nil
}

// Clean removes all the stale treeTables from the disk
func (cow *cowForest) clean() error {
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
