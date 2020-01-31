package utreexo

import "fmt"

// Full Pollard is the full representation of the utreexo forest, using
// binary tree pointers. It stores the location of the leaf (aka polNode)
// with a map like forest.
// While Pollard prunes nodes, full pollard stores all

// Full Pollard :
//
// Differs from Forest in that it uses binary tree pointer system
// instead of storing every node sequentially.
// Differs from Pollard in that it doesn't prune children/nieces
type FullPollard struct {
	numLeaves uint64 // number of leaves in the pollard forest

	tops []polNode // slice of the tree tops, which are polNodes.
	// tops are in big to small order
	// BUT THEY'RE WEIRD!  The left / right children are actual children,
	// not nieces as they are in every lower level.
	// In FullPollard, all the children exist, non are nil

	hashesEver, rememberEver, overWire uint64
}

// PolNode is a node in the pollard forest
// data can either be the sha256 hash of its children or
// the hash of the txo it represents.
// niece is needed as that is the proof for a polNode
// There can never be a nil child or niece for FullPollard
type polNode struct {
	data  Hash
	child [2]*polNode
	niece [2]*polNode
}

// auntOp returns the hash of a node's nieces. Never crashes for FullPollard
func (n *polNode) auntOp() Hash {
	return Parent(n.niece[0].data, n.niece[1].data)
}

// deadNode returns true if both children are nil
func (n *polNode) deadNode() bool {
	return n.child[0] == nil && n.child[1] == nil
}

// chop turns a node into a deadEnd by setting both child to nil.
func (n *polNode) chop() {
	n.child[0] = nil
	n.child[1] = nil
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

func (p *Pollard) height() uint8 { return treeHeight(p.numLeaves) }

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
