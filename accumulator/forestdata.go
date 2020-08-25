package accumulator

import (
	"fmt"
	"os"
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

// FileType represents what type a file on disk can be
type FileType int

const (
	// Utxo represents a six-tree which contain the hashes of the UTXOs
	// A leaf represents the row zero nodes
	// Example:
	//
	// 12
	// |-------\
	// 08      09
	// |---\   |---\
	// 00  01  02  03 <<-- These nodes are the UTXOs
	Utxo FileType = iota

	// TreeBlock is any tree with 127 leaves that does not contain the
	// actual hashes of UTXOs
	TreeBlock
)

type TreeBlockState int

const (
	// dirty is the cache bit for treeBlocks that have changed in memory
	// and thus must be updated in disk
	dirty TreeBlockState = iota

	// current is if the treeBlock is part of the latest Utreexo forest
	// state. Necessary as utreexo forest is a copy-on-write and doesn't
	// delete the old state
	current

	// sparse marks the current treeBlock as having empty nodes
	sparse

	// bottom indicates that this treeBlock contains the UTXO hashes
	bottom
)

type CowForestError int

const (
	PosTooBig CowForestError = iota
)

// Map of ErrorCode values back to their constant names for pretty printing.
var errorCodeStrings = map[CowForestError]string{
	PosTooBig: "Position too big",
}

type RuleError struct {
	ErrorCode   CowForestError // Describes the kind of error
	Description string         // Human readable description of the issue
}

func Set(b, flag TreeBlockState) TreeBlockState    { return b | flag }
func Clear(b, flag TreeBlockState) TreeBlockState  { return b &^ flag }
func Toggle(b, flag TreeBlockState) TreeBlockState { return b ^ flag }
func Has(b, flag TreeBlockState) bool              { return b&flag != 0 }

// memTable is the cache always in memory for a utreexo forest
type memTable struct {
	// treeElement are the utreexo tree
	treeElement treeElement

	// TreeTable caches
	cachedTreeTable []treeTable
}

func (m *memTable) read(pos uint64, forestRows uint8) (Hash, error) {
	// Check if stored tree is the lowest
	if forestRows <= 6 {
		if pos > 64 {
			return Hash{}, fmt.Errorf("CowForestError.PosTooBig")
		} else {
			hash, _ := m.treeElement.read(pos)
			return hash, nil
		}
	}
	depth, path, err := getPosition(pos, forestRows)
	if err != nil {
		return Hash{}, err
	}

	var leafToReturn treeElement
	leafToReturn = m.treeElement
	var hash Hash
	for i := uint8(0); i <= depth; i++ {
		hash, leafToReturn = m.treeElement.read(path[i])
	}
	return hash, nil

}

/*
func fetchLeaf(index uint64, treeElement treeElement) treeBlockLeaf {
	check := treeElement.read(index)
	if check.data != empty {
		return check
	}
}
*/

/*
func getGlobalPos(rows uint8, treeblockPos uint64, posInTreeBlock uint64) uint64 {
	times := uint64(1 << rows)
	globalPos := times*treeblockPos + posInTreeBlock
	return globalPos
}
*/

/*
func getPosInTreeBlock(gPos uint64, treeblockPos uint64, rows uint8) uint64 {
	times := uint64(1 << rows)
	posInTreeBlock := gPos - (treeblockPos * times)
	return posInTreeBlock
}
*/

// getPosition, given the global position of a leaf and the forestRows, returns
// what depth the treeBlock is and the position of the leaf within the treeBlocks.
// posInTreeBlock starts from the bottom and goes to the top. Will be nil if
// forestRows is less than or equal to 6
// Ex: For a forestRows = 14
// posInTreeBlock = [posForSixTree, posForTwelveTree]
func getPosition(globalPos uint64, forestRows uint8) (
	treeBlockDepth uint8, posInTreeBlock []uint64, err error) {

	if 1<<forestRows < globalPos {
		err = fmt.Errorf("position requested is greater than what the tree can hold")
		return
	}

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

// getPosInTreeBlock returns the position within a given treeBlock count
// and the given globalPos
func getPosInTreeBlock(globalPos uint64, forestRows, treeBlock uint8) uint8 {
	/*
		// Find the next power of 2
		globalPos--
		globalPos |= globalPos >> 1
		globalPos |= globalPos >> 2
		globalPos |= globalPos >> 4
		globalPos |= globalPos >> 8
		globalPos |= globalPos >> 16
		globalPos |= globalPos >> 32
		globalPos++

		// log2
		baseRow := uint8(bits.TrailingZeros64(globalPos) & ^int(64))
		return uint64(baseRow - treeBlock)
	*/

	// prevent overflows
	if treeBlock >= treeRows(globalPos) {
		return 0
	}
	return treeRows(globalPos) - treeBlock
}

type treeElement interface {
	read(index uint64) (Hash, treeBlock)
}

// treeBlockLeaf is a leaf in any given treeBlock and represents the lowest
// nodes in a given tree
type treeBlockLeaf struct {
	data      Hash
	treeBlock *treeBlock
}

// treeBlock is a representation of a row 6 utreexo tree.
// It contains 127 leaves and 32 bytes of metadata
// The leaves maybe be a UTXO hash or another treeBlock
type treeBlock struct {
	// TODO Lots of things we can do with 32 bytes
	//meta [32]byte

	// offset is the place of this treeBlock within a treeTable on disk
	// This is part of the 32byte metadata
	offset uint8

	// row is the row number for the heighest row in this treeBlock
	// The bottommost treeBlocks in the entire Utreexo tree will have a
	// row value of 6
	row uint8

	// meta is bitflags to represent the state of the treeBlock
	meta TreeBlockState

	// leaves are the utreexo nodes that are either
	// 1: Hash of a UTXO
	// 2: Parent hash of two nodes
	// 3: another treeBlock
	//
	// Able to hold up to 64 leaves
	leaves [127]treeBlockLeaf
}

// read takes a leaf position of *this* treeBlock and returns the value
// It checks the memory cache first then goes to disk.
func (tb *treeBlock) read(pos uint64) (Hash, treeBlock) {
	if Has(tb.meta, bottom) {

	}

	return Hash{}, nil
}

// treeTable is a group of treeBlocks that are sorted on disk
type treeTable struct {
	// Each treeBlock is exactly 4096 bytes. 256 of them are 1 Mebibyte
	// NOTE 256 was chosen as treeBlock offset of uint8 can store
	// 256 numbers. uint16 is wasting a quite a few bits
	cachedTreeBlocks [256]treeBlock

	// The file on disk containing the treeBlocks
	file *os.File
}

// cowForest is a wrapper around memTable, treeTable, and treeBlock
// Needed mostly for compatibility with Forestdata.
// Shorthand for copy-on-write. Unfortuntely, it doesn't go moo
type CowForest struct {
	MemTable  memTable
	TreeTable treeTable
	TreeBlock treeBlock
}

// Read takes a position of a leaf and returns the value of it
func (cow *CowForest) Read(pos uint64) Hash {
	/*
		_, err := cow.TreeTable.read(pos)
		if err != nil {
			// TODO really doesn't need to panic though
			// Modify the ForestData to return an error or handle it
			// differently
			panic(err)
		}
	*/
	return Hash{}
}

func (cow *CowForest) write() {

}

// GetSnapshot returns a snapshot of the forest in a given height
func (cow *CowForest) GetSnapshot(blockHeight int32) *CowForest {
	// TODO placeholder
	return &CowForest{}
}

// Flush writes everything to the disk
func (cow *CowForest) Flush() {
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
