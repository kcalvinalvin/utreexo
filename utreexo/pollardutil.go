package utreexo

import (
	"fmt"
	"os"

	"github.com/mit-dci/utreexo/cmd/simutil"
)

// In pollard, there are two variations of Pollard, FullPollard and Pollard.
// The differences are in that whether they store all the nodes or prune them
// to save on storage.

// Full Pollard may have a tree like so

// 28
// |---------------\
// 24              25              26
// |-------\       |-------\       |-------\
// 16      17      18      19      20      21      22
// |---\   |---\   |---\   |---\   |---\   |---\   |---\
// 00  01  02  03  04  05  06  07  08  09  10  11  12  13  14

// Pollard prunes a lot of nodes so you may end up with a tree
// that looks like so where only the tree tops are kept.

// 28
// |---------------\
//                                 26
// |-------\       |-------\       |-------\
//                                                 22
// |---\   |---\   |---\   |---\   |---\   |---\   |---\
//                                                         14

// There may be different variations of this pollard forest
// (like in the case where you store some nodes for caching purposes).

// Full Pollard is the full representation of the utreexo forest, using
// binary tree pointers. It stores the location of the leaf (aka polNode)
// with a map like forest.
// While Pollard prunes nodes, full pollard stores every node.
// Differs from Forest in that it uses binary tree pointer system
// instead of storing every node sequentially.
// Differs from Pollard in that it doesn't prune nieces.
type FullPollard struct {
	numLeaves uint64 // number of leaves in the pollard forest

	tops []polNode // slice of the tree tops, which are polNodes.
	// tops are in big to small order
	// BUT THEY'RE WEIRD!  The left / right children are actual children,
	// not nieces as they are in every lower level.
	// In FullPollard, all the children exist, non are nil

	hashesEver, overWire uint64
}

// Pollard is the sparse representation of the utreexo forest, using
// binary tree pointers instead of a hash map.

// I generally avoid recursion as much as I can, using regular for loops and
// ranges instead.  That might start looking pretty contrived here, but
// I'm still going to try it.

// Pollard :
type Pollard struct {
	numLeaves uint64 // number of leaves in the pollard forest

	tops []polNode // slice of the tree tops, which are polNodes.
	// tops are in big to small order
	// BUT THEY'RE WEIRD!  The left / right children are actual children,
	// not nieces as they are in every lower level.

	hashesEver, rememberEver, overWire uint64

	//	Lookahead int32  // remember leafs below this TTL
	//	Minleaves uint64 // remember everything below this leaf count
}

// PolNode is a node in the pollard forest
// data can either be the sha256 hash of its children or
// the hash of the txo it represents.
// niece is needed as that is the proof for a polNode
// There can never be a nil child or niece for FullPollard
type polNode struct {
	data  Hash
	niece [2]*polNode
}

// auntOp returns the hash of a nodes nieces. crashes if you call on nil nieces.
// never crashes for FullPollard
func (n *polNode) auntOp() Hash {
	return Parent(n.niece[0].data, n.niece[1].data)
}

// auntable tells you if you can call auntOp on a node
func (n *polNode) auntable() bool {
	return n.niece[0] != nil && n.niece[1] != nil
}

// deadEnd returns true if both nieces are nil
// could also return true if n itself is nil! (maybe a bad idea?)
func (n *polNode) deadEnd() bool {
	// if n == nil {
	// 	fmt.Printf("nil deadend\n")
	// 	return true
	// }
	return n.niece[0] == nil && n.niece[1] == nil
}

// chop turns a node into a deadEnd by setting both nieces to nil.
func (n *polNode) chop() {
	n.niece[0] = nil
	n.niece[1] = nil
}

//  printout printfs the node
func (n *polNode) printout() {
	if n == nil {
		fmt.Printf("nil node\n")
		return
	}
	fmt.Printf("Node %x ", n.data[:4])
	if n.niece[0] == nil {
		fmt.Printf("l nil ")
	} else {
		fmt.Printf("l %x ", n.niece[0].data[:4])
	}
	if n.niece[1] == nil {
		fmt.Printf("r nil\n")
	} else {
		fmt.Printf("r %x\n", n.niece[1].data[:4])
	}
	return
}

// prune prunes deadend children.
// don't prune at the bottom; use leaf prune instead at height 1
func (n *polNode) prune() {
	if n.niece[0].deadEnd() {
		n.niece[0] = nil
	}
	if n.niece[1].deadEnd() {
		n.niece[1] = nil
	}
}

// leafPrune is the prune method for leaves.  You don't want to chop off a leaf
// just because it's not memorable; it might be there because its sibling is
// memorable.  Call this at height 1 (not 0)
func (n *polNode) leafPrune() {
	if n.niece[0] != nil && n.niece[1] != nil &&
		n.niece[0].deadEnd() && n.niece[1].deadEnd() {
		n.chop()
	}
}

// polSwap swaps the contents of two polNodes & leaves pointers to them intact
// need their siblings so that the siblings' neices can swap.
// for a top, just say the top's sibling is itself and it should work.
func polSwap(a, asib, b, bsib *polNode) error {
	if a == nil || asib == nil || b == nil || bsib == nil {
		return fmt.Errorf("polSwap given nil node")
	}
	a.data, b.data = b.data, a.data
	asib.niece, bsib.niece = bsib.niece, asib.niece
	return nil
}

func (p *Pollard) height() uint8      { return treeHeight(p.numLeaves) }
func (fp *FullPollard) height() uint8 { return treeHeight(fp.numLeaves) }

// TopHashesReverse is ugly and returns the top hashes in reverse order
// ... which is the order full forest is using until I can refactor that code
// to make it big to small order
func (p *Pollard) topHashesReverse() []Hash {
	rHashes := make([]Hash, len(p.tops))
	for i, n := range p.tops {
		rHashes[len(rHashes)-(1+i)] = n.data
	}
	return rHashes
}

func (p *FullPollard) topHashesReverse() []Hash {
	rHashes := make([]Hash, len(p.tops))
	for i, n := range p.tops {
		rHashes[len(rHashes)-(1+i)] = n.data
	}
	return rHashes
}

func (p *Pollard) WritePollard(pollardFile *os.File) error {

	// "Overwriting" pollardFile passed in
	// I feel like this is faster than writing 0s
	os.Remove(simutil.PollardFilePath)
	var err error
	pollardFile, err = os.OpenFile(
		simutil.PollardFilePath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}

	// The Hash of all the tops appended
	var allTops []byte
	for _, t := range p.tops {
		allTops = append(allTops, t.data[:]...)
	}

	_, err = pollardFile.WriteAt(append(U64tB(p.numLeaves), allTops...), 0)
	if err != nil {
		return err
	}
	fmt.Println("Pollard leaves:", p.numLeaves)
	return nil
}

func (p *Pollard) RestorePollard(pollardFile *os.File) error {
	fmt.Println("Restoring Pollard Roots...")

	var byteLeaves [8]byte
	_, err := pollardFile.Read(byteLeaves[:])
	if err != nil {
		panic(err)
	}
	p.numLeaves = BtU64(byteLeaves[:])
	fmt.Println("Pollard Leaves:", p.numLeaves)

	pstat, err := pollardFile.Stat()
	if err != nil {
		panic(err)
	}

	pos := int64(8)
	for i := int(0); pos != pstat.Size(); i++ {
		var h Hash
		_, err := pollardFile.Read(h[:])
		if err != nil {
			panic(err)
		}
		n := new(polNode)
		n.data = h
		p.tops = append(p.tops, *n)
		pos += 32
	}
	fmt.Println("Finished restoring pollard")
	return nil
}
func (p *FullPollard) WritePollard(pollardFile *os.File) error {

	// "Overwriting" pollardFile passed in
	// I feel like this is faster than writing 0s
	os.Remove(simutil.PollardFilePath)
	var err error
	pollardFile, err = os.OpenFile(
		simutil.PollardFilePath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}

	// The Hash of all the tops appended
	var allTops []byte
	for _, t := range p.tops {
		allTops = append(allTops, t.data[:]...)
	}

	_, err = pollardFile.WriteAt(append(U64tB(p.numLeaves), allTops...), 0)
	if err != nil {
		return err
	}
	fmt.Println("Pollard leaves:", p.numLeaves)
	return nil
}

func (p *FullPollard) RestorePollard(pollardFile *os.File) error {
	fmt.Println("Restoring Pollard Roots...")

	var byteLeaves [8]byte
	_, err := pollardFile.Read(byteLeaves[:])
	if err != nil {
		panic(err)
	}
	p.numLeaves = BtU64(byteLeaves[:])
	fmt.Println("Pollard Leaves:", p.numLeaves)

	pstat, err := pollardFile.Stat()
	if err != nil {
		panic(err)
	}

	pos := int64(8)
	for i := int(0); pos != pstat.Size(); i++ {
		var h Hash
		_, err := pollardFile.Read(h[:])
		if err != nil {
			panic(err)
		}
		n := new(polNode)
		n.data = h
		p.tops = append(p.tops, *n)
		pos += 32
	}
	fmt.Println("Finished restoring pollard")
	return nil
}
