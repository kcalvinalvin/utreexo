package accumulator

import (
	"fmt"

	"github.com/mit-dci/utreexo/accumulator/util"
)

// IngestBlockProof populates the Pollard with all needed data to delete the
// targets in the block proof
func (p *Pollard) IngestBatchProof(bp BatchProof) error {
	var empty util.Hash
	// TODO so many things to change
	ok, proofMap := VerifyBatchProof(
		bp, p.topHashesReverse(), p.numLeaves, p.height())
	if !ok {
		return fmt.Errorf("block proof mismatch")
	}
	//	fmt.Printf("targets: %v\n", bp.Targets)
	// go through each target and populate pollard
	for _, target := range bp.Targets {

		tNum, branchLen, bits := util.DetectOffset(target, p.numLeaves)
		if branchLen == 0 {
			// if there's no branch (1-tree) nothing to prove
			continue
		}
		node := &p.tops[tNum]
		h := branchLen - 1
		pos := util.UpMany(target, branchLen, p.height()) // this works but...
		// we should have a way to get the top positions from just p.tops

		// fmt.Printf("ingest adding target %d to top %04x h %d brlen %d bits %04b\n",
		// target, node.data[:4], h, branchLen, bits&((2<<h)-1))

		lr := (bits >> h) & 1
		pos = (util.Child(pos, p.height())) | lr
		// descend until we hit the bottom, populating as we go
		// also populate siblings...
		for {
			if node.niece[lr] == nil {
				node.niece[lr] = new(PolNode)
				node.niece[lr].data = proofMap[pos]
				// fmt.Printf("------wrote %x at %d\n", proofMap[pos], pos)
				if node.niece[lr].data == empty {
					return fmt.Errorf(
						"h %d wrote empty hash at pos %d %04x.niece[%d]",
						h, pos, node.data[:4], lr)
				}
				// fmt.Printf("h %d wrote %04x to %d\n", h, node.niece[lr].data[:4], pos)
				p.overWire++
			}
			if node.niece[lr^1] == nil {
				node.niece[lr^1] = new(PolNode)
				node.niece[lr^1].data = proofMap[pos^1]
				// doesn't count as overwire because computed, not read
			}

			if h == 0 {
				break
			}
			h--
			node = node.niece[lr]
			lr = (bits >> h) & 1
			pos = (util.Child(pos, p.height()) ^ 2) | lr
		}

		// TODO do you need this at all?  If the Verify part already happened, maybe not?
		// at bottom, populate target if needed
		// if we don't need this and take it out, will need to change the forget
		// pop above

		if node.niece[lr^1] == nil {
			node.niece[lr^1] = new(PolNode)
			node.niece[lr^1].data = proofMap[pos^1]
			fmt.Printf("------wrote %x at %d\n", proofMap[pos^1], pos^1)
			if node.niece[lr^1].data == empty {
				return fmt.Errorf("Wrote an empty hash h %d under %04x %d.niece[%d]",
					h, node.data[:4], pos, lr^1)
			}
			// p.overWire++ // doesn't count...? got it for free?
		}
	}
	return nil
}
