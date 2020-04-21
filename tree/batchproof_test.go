package tree

import (
	"fmt"
	"testing"

	"github.com/mit-dci/utreexo/util"
)

// TestVerifyBlockProof tests that the computedTop is compared to the top in the
// Utreexo forest.
func TestVerifyBatchProof(t *testing.T) {
	// Create forest in memory
	f := NewForest(nil)

	// last index to be deleted. Same as blockDels
	lastIdx := uint64(7)

	// Generate adds
	adds := make([]util.LeafTXO, 8)
	adds[0].Hash = util.Hash{1}
	adds[1].Hash = util.Hash{2}
	adds[2].Hash = util.Hash{3}
	adds[3].Hash = util.Hash{4}
	adds[4].Hash = util.Hash{5}
	adds[5].Hash = util.Hash{6}
	adds[6].Hash = util.Hash{7}
	adds[7].Hash = util.Hash{8}

	// Modify with the additions to simulate txos being added
	_, err := f.Modify(adds, nil)
	if err != nil {
		t.Fatal(err)
	}

	// create blockProof based on the last add in the slice
<<<<<<< HEAD:tree/blockproof_test.go
	blockProof, err := f.ProveBlock(
		[]util.Hash{adds[lastIdx].Hash})
=======
	blockProof, err := f.ProveBatch(
		[]Hash{adds[lastIdx].Hash})
>>>>>>> master:tree/batchproof_test.go

	if err != nil {
		t.Fatal(err)
	}

	// Confirm that verify block proof works
	shouldBetrue := f.VerifyBatchProof(blockProof)
	if shouldBetrue != true {
		t.Fail()
		t.Logf("Block failed to verify")
	}

	// delete last leaf and add a new leaf
	adds = make([]util.LeafTXO, 1)
	adds[0].Hash = util.Hash{9}
	_, err = f.Modify(adds, []uint64{lastIdx})
	if err != nil {
		t.Fatal(err)
	}

	// Attempt to verify block proof with deleted element
	shouldBeFalse := f.VerifyBatchProof(blockProof)
	if shouldBeFalse != false {
		t.Fail()
		t.Logf("Block verified with old proof. Double spending allowed.")
	}
}

// In a two leaf tree:
// We prove one node, then delete the other one.
// Now, the proof of the first node should not pass verification.

// Full explanation: https://github.com/mit-dci/utreexo/pull/95#issuecomment-599390850
func TestProofShouldNotValidateAfterNodeDeleted(t *testing.T) {
	adds := make([]util.LeafTXO, 2)
	proofIndex := 1
	adds[0].Hash = util.Hash{1} // will be deleted
	adds[1].Hash = util.Hash{2} // will be proven

	f := NewForest(nil)
	_, err := f.Modify(adds, nil)
	if err != nil {
		t.Fatal(fmt.Errorf("Modify with initial adds: %v", err))
	}

<<<<<<< HEAD:tree/blockproof_test.go
	blockProof, err := f.ProveBlock(
		[]util.Hash{
=======
	batchProof, err := f.ProveBatch(
		[]Hash{
>>>>>>> master:tree/batchproof_test.go
			adds[proofIndex].Hash,
		})
	if err != nil {
		t.Fatal(fmt.Errorf("ProveBlock of existing values: %v", err))
	}

	if !f.VerifyBatchProof(batchProof) {
		t.Fatal(
			fmt.Errorf(
				"proof of %d didn't verify (before deletion)",
				proofIndex))
	}

	_, err = f.Modify(nil, []uint64{0})
	if err != nil {
		t.Fatal(fmt.Errorf("Modify with deletions: %v", err))
	}

	if f.VerifyBatchProof(batchProof) {
		t.Fatal(
			fmt.Errorf(
				"proof of %d is still valid (after deletion)",
				proofIndex))
	}
}
