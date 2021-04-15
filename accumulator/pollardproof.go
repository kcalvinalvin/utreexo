package accumulator

import (
	"fmt"
)

// blazeLeaves "blazes" a trail to all leaves, creating empty nodes and
// siblings along the way if needed.
// Returns a 2d slice of polNodes sorted by: treePosition, branchLen
// The row designates what root all these nodes are under.
func (p *Pollard) populateAndCheck(leafPositions []uint64) ([][]*polNode, error) {
	// make sure the positions are sorted
	sortUint64s(leafPositions)

	positionList := NewPositionList()
	defer positionList.Free()

	computablePositions := NewPositionList()
	defer computablePositions.Free()

	ProofPositions(leafPositions, p.numLeaves, p.rows(), &positionList.list, &computablePositions.list)

	fmt.Println(positionList.list)
	fmt.Println(computablePositions.list)

	// positionList pointer. Keeps track of which positionList we're at.
	// goes in reverse since we're looping through the computablePositions in reverse
	// as well
	pp := len(positionList.list) - 1

	// prevBranchLen keeps track of how far we're down a tree. Since utreexo is made of
	// many trees, we need a slice to keep track of all of the progress
	prevBranchLen := make([]uint8, len(p.roots))

	// polNodes are pointers to the nodes that should be hashed. The row designates what =
	// root the nodes are under. In a row, there are all the nodes that need to be hashed.
	// The nodes are organized like how a typical binary tree would be organized in a
	// slice (or array). Parent of 2 nodes are calculated by index/2 and the children
	// are computed by index*2 and index*2+1.
	//
	polNodes := make([]*[]*polNode, len(p.roots)) // len(positionList.list)+len(computablePositions.list))

	for i := 0; i < len(polNodes); i++ {
		r := make([]*polNode, 3)
		fmt.Println("p.roots[i]", p.roots[i])
		r[0] = p.roots[i]
		r[1] = p.roots[i].niece[0]
		r[2] = p.roots[i].niece[1]
		polNodes[i] = &r
	}

	// reverse iter through all the computablePositions. Since the computablePositions
	// are in order, we end up going from the heighest node (root or some node very close to a root)
	// to the lowest (a leaf).
	for i := len(computablePositions.list) - 1; i >= 0; i-- {
		tree, branchLen, bits := detectOffset(computablePositions.list[i], p.numLeaves)
		fmt.Println(tree, branchLen, computablePositions.list[i])

		// if branchLen is 0, then we're at root. Move on
		// if it's not 0, then it means we're at a non-root polNode
		if branchLen == 0 {
			continue
		} else {
			// computablePositions should only move down to the leaves. If
			// we somehow moved up, then panic
			// NOTE this should never happen
			// TODO this could be abused and a node could be shut down with this.
			// Actually handle the error
			if branchLen < prevBranchLen[tree] {
				str := fmt.Errorf("branchLen %d smaller than prevBranchLen %d\n", branchLen, prevBranchLen)
				panic(str)
			}
			prevBranchLen[tree] = branchLen
		}

		//currentHighestNode, currentHighestNodeSib := polNodes[tree][0], polNodes[tree][0]
		nodeRowIdx := 0

		var n, nsib *polNode
		n, nsib = (*polNodes[tree])[nodeRowIdx], (*polNodes[tree])[nodeRowIdx]
		//var currentHighestNode, currentHighestNodeSib *polNode
		// we start with 3 polNodes in a row. If more are there, it means we've
		// already hit a nil node
		//if len(*polNodes[tree]) <= 3 {
		//	// Grab the currentHighestNode that's not root
		//	currentHighestNode = (*polNodes[tree])[1]
		//	currentHighestNodeSib = (*polNodes[tree])[2]
		//} else {
		//	// TODO use bits to find the currentHighestNode
		//}

		// init. Just have both point to the same thing
		//n, nsib := currentHighestNode, currentHighestNodeSib

		//moveDownCount := branchLen - prevBranchLen[tree]

		// **Grab the node
		// move down the tree and grab the node
		//for h := moveDownCount - 1; h != 0; h-- {
		for h := branchLen - 1; h != 0; h-- {
			//polNodes[tree][nodeRowIdx*2]
			// TODO use bits to find the currentHighestNode
			// NOTE RIGHT NOW IT"S WRONG
			// get the positions
			nPos := uint8(bits>>h) & 1
			nSibPos := nPos ^ 1

			// Grab the nodes
			//n, nsib = currentHighestNode.niece[nPos], currentHighestNode.niece[nSibPos]
			n, nsib = n.niece[nPos], n.niece[nSibPos]

			// If the node or its sibilng is nil, means that we pruned it off.
			// Need to make a new one
			if n.niece[nSibPos] == nil {
				n.niece[nSibPos] = &polNode{}
			}

			if nsib == nil {
				nsib = &polNode{}
			}

			if n == nil {
				n = &polNode{}
			}
		}
		lr := uint8(bits) & 1
		lrSib := lr ^ 1
		// switch siblingness. This is weird but it's because we point to nieces not
		// children. The moving down the tree is a bit zig-zaggy.
		n, nsib = n.niece[lrSib], n.niece[lr]
		if nsib == nil {
			nsib = &polNode{}
		}

		if n == nil {
			n = &polNode{}
		}
		// **Grab the node

		// Now we have the node. Check if the data for it is empty. If it isn't
		// then this is the next heighest populated node.
		if n.data != empty {
		} else {
		}

		// if we're already through with all the proofPositions, don't bother
		// checking
		if pp >= 0 {
			fmt.Println(computablePositions.list[i], positionList.list[pp])
			// check if they're sibilings
			if computablePositions.list[i]^1 == positionList.list[pp] ||
				positionList.list[pp]^1 == computablePositions.list[i] {
				pp--
			}
		}
	}

	if pp != -1 {
		err := fmt.Errorf("Did not get to all the proofPositions. "+
			"%v proofPositions did not get checked", pp)
		panic(err)
	}

	return nil, nil
}

// IngestBatchProof populates the Pollard with all needed data to delete the
// targets in the block proof
func (p *Pollard) IngestBatchProof(bp BatchProof) error {
	// verify the batch proof.
	rootHashes := p.rootHashesForward()
	ok, trees, roots := verifyBatchProof(bp, rootHashes, p.numLeaves,
		// pass a closure that checks the pollard for cached nodes.
		// returns true and the hash value of the node if it exists.
		// returns false if the node does not exist or the hash value is empty.
		func(pos uint64) (bool, Hash) {
			n, _, _, err := p.readPos(pos)
			if err != nil {
				return false, empty
			}
			if n != nil && n.data != empty {
				return true, n.data
			}

			return false, empty
		})
	if !ok {
		return fmt.Errorf("block proof mismatch")
	}

	p.populateAndCheck(bp.Targets)
	//p.blazeLeaves(bp.Targets)
	//p.ingestAndCheck(bp, bp.Proof)

	// preallocating polNodes helps with garbage collection
	polNodes := make([]polNode, len(trees)*3)

	fmt.Println("pollard height", p.rows())
	// rootIdx and rootIdxBackwards is needed because p.populate()
	// expects the roots in a reverse order. Thus the need for two
	// indexes. TODO fix this to have only one index
	rootIdx := len(rootHashes) - 1
	rootIdxBackwards := 0
	nodesAllocated := 0
	for _, root := range roots {
		for root.Val != rootHashes[rootIdx] {
			rootIdx--
			rootIdxBackwards++
		}
		// populate the pollard
		nodesAllocated += p.populate(p.roots[(len(p.roots)-rootIdxBackwards)-1],
			root.Pos, trees, polNodes[nodesAllocated:])
	}

	return nil
}

// populate takes a root and populates it with the nodes of the paritial proof tree that was computed
// in `verifyBatchProof`.
func (p *Pollard) populate(root *polNode, pos uint64, trees [][3]node, polNodes []polNode) int {
	var looped int
	// a stack to traverse the pollard
	type stackElem struct {
		trees [][3]node
		node  *polNode
		pos   uint64
	}
	stack := make([]stackElem, 0, len(trees))
	stack = append(stack, stackElem{trees, root, pos})
	rows := p.rows()
	nodesAllocated := 0
	for len(stack) > 0 {
		looped++
		elem := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if elem.pos < p.numLeaves {
			// this is a leaf, we are done populating this branch.
			continue
		}

		leftChild := child(elem.pos, rows)
		rightChild := child(elem.pos, rows) | 1
		var left, right *polNode
		i := len(elem.trees) - 1
	find_nodes:
		for ; i >= 0; i-- {
			switch elem.trees[i][0].Pos {
			case elem.pos:
				fallthrough
			case rightChild:
				if elem.node.niece[0] == nil {
					elem.node.niece[0] = &polNodes[nodesAllocated]
					nodesAllocated++
				}
				right = elem.node.niece[0]
				right.data = elem.trees[i][1].Val
				fallthrough
			case leftChild:
				if elem.node.niece[1] == nil {
					elem.node.niece[1] = &polNodes[nodesAllocated]
					nodesAllocated++
				}
				left = elem.node.niece[1]
				left.data = elem.trees[i][2].Val
				break find_nodes
			}
		}
		if i < 0 {
			continue
		}

		stack = append(stack,
			stackElem{trees[:i], left, leftChild}, stackElem{trees[:i], right, rightChild})
	}

	fmt.Println("populate looped this many times: ", looped)
	return nodesAllocated
}

// ingestAndCheck puts the targets and proofs from the BatchProof into the
// pollard, and computes parents as needed up to already populated nodes.
func (p *Pollard) ingestAndCheck(bp BatchProof, targs []Hash) error {
	if len(targs) == 0 {
		return nil
	}
	// if bp targs and targs have different length, this will crash.
	// they shouldn't though, make sure there are previous checks for that

	//maxpp := len(bp.Proof)
	//pp := 0 // proof pointer; where we are in the pointer slice
	// instead of popping like bp.proofs = bp.proofs[1:]

	rows := p.rows()

	fmt.Printf("got proof %s\n", bp.ToString())

	positionList := NewPositionList()
	defer positionList.Free()

	computablePositions := NewPositionList()
	defer computablePositions.Free()

	ProofPositions(bp.Targets, p.numLeaves, rows, &positionList.list, &computablePositions.list)

	fmt.Println(positionList.list)
	fmt.Println(computablePositions.list)

	// targetNodes holds nodes that are known, on the bottom row those
	// are the targets, on the upper rows it holds computed nodes.
	// rootCandidates holds the roots that where computed, and have to be
	// compared to the actual roots at the end.
	targetNodes := make([]node, 0, len(bp.Targets)*int(rows))

	// initialise the targetNodes for row 0.
	// TODO: this would be more straight forward if bp.Proofs wouldn't
	// contain the targets
	proofHashes := make([]Hash, 0, len(positionList.list))
	var targetsMatched uint64
	for len(bp.Targets) > 0 {
		// check if the target is the row 0 root.
		// this is the case if its the last leaf (pos==numLeaves-1)
		// AND the tree has a root at row 0 (numLeaves&1==1)
		if bp.Targets[0] == p.numLeaves-1 && p.numLeaves&1 == 1 {
			//// target is the row 0 root, append it to the root candidates.
			//rootCandidates = append(rootCandidates,
			//	node{Val: roots[len(roots)-1], Pos: targets[0]})
			bp.Proof = bp.Proof[1:]
			break
		}

		// `targets` might contain a target and its sibling or just the target, if
		// only the target is present the sibling will be in `proofPositions`.
		if uint64(len(positionList.list)) > targetsMatched &&
			bp.Targets[0]^1 == positionList.list[targetsMatched] {
			// the sibling of the target is included in the proof positions.
			lr := bp.Targets[0] & 1
			targetNodes = append(targetNodes, node{Pos: bp.Targets[0], Val: bp.Proof[lr]})
			proofHashes = append(proofHashes, bp.Proof[lr^1])
			targetsMatched++
			bp.Proof = bp.Proof[2:]
			bp.Targets = bp.Targets[1:]
			continue
		}

		// the sibling is not included in the proof positions, therefore
		// it has to be included in targets. if there are less than 2 proof
		// hashes or less than 2 targets left the proof is invalid because
		// there is a target without matching proof.
		if len(bp.Proof) < 2 || len(bp.Targets) < 2 {
			panic("wrogn")
			//return false, nil, nil
		}

		targetNodes = append(targetNodes,
			node{Pos: bp.Targets[0], Val: bp.Proof[0]},
			node{Pos: bp.Targets[1], Val: bp.Proof[1]})
		bp.Proof = bp.Proof[2:]
		bp.Targets = bp.Targets[2:]
	}

	fmt.Println(len(targetNodes), targetNodes)

	for len(targetNodes) > 0 {
		// Grab the tree that the position is at
		tree, _, _ := detectOffset(targetNodes[0].Pos, p.numLeaves)
		fmt.Println("tree: ", tree, targetNodes[0].Pos)
		targetNodes = targetNodes[1:]
	}

	//// targetNodes are the single row of nodes that are to be deleted. We move up
	//// the pollard forest
	//for len(targetNodes) > 0 {
	//	var target, proof node
	//	target = targetNodes[0]
	//	if len(positionList.list) > 0 && target.Pos^1 == positionList.list[0] {
	//		// target has a sibling in the proof positions, fetch proof
	//		proof = node{Pos: positionList.list[0], Val: bp.Proof[0]}
	//		positionList.list = positionList.list[1:]
	//		bp.Proof = bp.Proof[1:]
	//		targetNodes = targetNodes[1:]
	//	} else {
	//		// target should have its sibling in targetNodes
	//		if len(targetNodes) == 1 {
	//			// sibling not found
	//			panic("wrong no sib")
	//		}

	//		proof = targetNodes[1]
	//		targetNodes = targetNodes[2:]
	//	}

	//	// figure out which node is left and which is right
	//	left := target
	//	right := proof
	//	if target.Pos&1 == 1 {
	//		right, left = left, right
	//	}

	//	// get the hash of the parent from the cache or compute it
	//	parentPos := parent(target.Pos, rows)
	//	isParentCached, cachedHash := cached(parentPos)
	//	hash := parentHash(left.Val, right.Val)
	//	if isParentCached && hash != cachedHash {
	//		// The hash did not match the cached hash
	//		return false, nil, nil
	//	}
	//}

	// the main thing ingestAndCheck does is write hashes to the pollard.
	// the hashes can come from 2 places: arguments or hashing.
	// for arguments, proofs and targets are treated pretty much the same;
	// read em off the slice and write em in.
	// any time you're writing somthing that's already there, check to make
	// sure it matches.  if it doesn't, return an error.
	// if it does, you don't need to hash any parents above that.

	// first range through targets, populating / matching, and placing proof
	// hashes if the targets are not twins

	//for i := 0; i < len(bp.Targets); i++ {
	//	targpos := bp.Targets[i]

	//	n, nsib, _, err := p.grabPos(targpos)
	//	if err != nil {
	//		return err
	//	}

	//	// Check if either are nil.
	//	if n == nil {
	//		n = &polNode{}
	//	}
	//	if nsib == nil {
	//		nsib = &polNode{}
	//	}

	//	err = matchPop(n, targs[i])
	//	if err != nil {
	//		return err
	//	}
	//	// If the node we're looking at matches the proof, then advance
	//	if n.data == bp.Proof[pp] {
	//		pp++
	//	}

	//	// see if current target is a twin target
	//	if i+1 < len(targs) && bp.Targets[i]|1 == bp.Targets[i+1] {
	//		err = matchPop(nsib, targs[i+1])
	//		if err != nil {
	//			return err
	//		}
	//		i++ // dealt with an extra target
	//	} else { // non-twin, needs proof
	//		if pp == maxpp {
	//			return fmt.Errorf("need more proofs")
	//		}
	//		err = matchPop(nsib, bp.Proof[pp])
	//		if err != nil {
	//			return err
	//		}
	//		pp++

	//		// If the proof we're looking at is equal to the target
	//		// then we advance the proof pointer
	//		if targs[i] == bp.Proof[pp] {
	//			pp++
	//		}

	//	}
	//}

	return nil
}

// quick function to populate, or match/fail
func matchPop(n *polNode, h Hash) error {
	if n.data == empty { // node was empty; populate
		n.data = h
		return nil
	}
	if n.data == h { // node was full & matches; OK
		return nil
	}
	// didn't match
	return fmt.Errorf("Proof doesn't match; expect %x, got %x", n.data, h)
}
