package csn

import (
	"fmt"
	"os"
	//"sync"
	"time"

	"github.com/btcsuite/btcutil"
	"github.com/mit-dci/utreexo/cmd/ttl"
	"github.com/mit-dci/utreexo/cmd/util"
	"github.com/mit-dci/utreexo/utreexo"
	"github.com/syndtr/goleveldb/leveldb"
)

// Here we write proofs for all the txs.
// All the inputs are saved as 32byte sha256 hashes.
// All the outputs are saved as LeafTXO type.
func genPollard(
	tx []*btcutil.Tx,
	height int32,
	totalTXOAdded *int,
	lookahead int32,
	totalDels *int,
	plustime time.Duration,
	pFile *os.File,
	pOffsetFile *os.File,
	lvdb *leveldb.DB,
	p *utreexo.Pollard) error {

	plusstart := time.Now()

	blockAdds, err := genAdds(tx, lvdb, height, lookahead)
	*totalTXOAdded += len(blockAdds)

	donetime := time.Now()
	plustime += donetime.Sub(plusstart)

	// Grab the proof by height
	bpBytes, err := getProof(uint32(height), pFile, pOffsetFile)
	if err != nil {
		return err
	}
	bp, err := utreexo.FromBytesBlockProof(bpBytes)
	if err != nil {
		return err
	}
	*totalDels += len(bp.Targets)

	// Fills in the nieces for verification/deletion
	err = p.IngestBlockProof(bp)
	if err != nil {
		return err
	}

	// Utreexo tree modification. blockAdds are the added txos and
	// bp.Targets are the positions of the leaves to delete
	err = p.Modify(blockAdds, bp.Targets)
	if err != nil {
		return err
	}
	return nil
}

func genAdds(txs []*btcutil.Tx, db *leveldb.DB,
	height int32, lookahead int32) (blockAdds []utreexo.LeafTXO, err error) {

	// grab all the MsgTx
	for blockIndex, tx := range txs {
		// Cache the txid as it's expensive to generate
		txid := tx.MsgTx().TxHash().String()

		var remaining int
		addChan := make(chan ttl.DeathInfo)
		// Start goroutines to fetch from leveldb
		for txIndex, out := range tx.MsgTx().TxOut {
			if util.IsUnspendable(out) {
				continue
			}
			remaining++
			go dbLookUp(txid, int32(txIndex),
				int32(blockIndex), height, lookahead, addChan, db)
		}
		// needed to receive in order
		txoAdds := make([]ttl.DeathInfo, remaining)
		for remaining > 0 {
			x := <-addChan
			if x.DeathHeight < 0 {
				panic("negative deathheight")
			}
			txoAdds[x.TxPos] = x
			remaining--
		}
		for _, x := range txoAdds {
			// Skip same block spends
			// deathHeight is where the tx is spent. Height+1 represents
			// what the current block height is
			if x.DeathHeight-(height+1) == 0 {
				continue
			}
			if x.DeathHeight == 0 {
				add := utreexo.LeafTXO{Hash: x.Txid}
				blockAdds = append(blockAdds, add)
			} else {
				add := utreexo.LeafTXO{
					Hash:     x.Txid,
					Duration: x.DeathHeight - (height + 1),
					Remember: x.DeathHeight-(height+1) < lookahead}
				blockAdds = append(blockAdds, add)
			}
		}
	}
	return blockAdds, nil
}

// Gets the proof for a given block height
func getProof(height uint32, pFile *os.File, pOffsetFile *os.File) ([]byte, error) {

	var offset [4]byte
	pOffsetFile.Seek(int64(height*4), 0)
	pOffsetFile.Read(offset[:])
	if offset == [4]byte{} && height != uint32(0) {
		panic(fmt.Errorf("offset returned nil\n" +
			"Likely that genproofs was exited before finishing\n" +
			"Run genproofs again and that will likely fix the problem"))
	}

	pFile.Seek(int64(util.BtU32(offset[:])), 0)

	var heightbytes [4]byte
	pFile.Read(heightbytes[:])

	var compare0 [4]byte
	copy(compare0[:], heightbytes[:])

	var compare1 [4]byte
	copy(compare1[:], utreexo.U32tB(height+1))
	//check if height matches
	if compare0 != compare1 {
		fmt.Println("read:, given:", compare0, compare1)
		return nil, fmt.Errorf("Corrupted proofoffset file\n")
	}

	var proofsize [4]byte
	pFile.Read(proofsize[:])

	proof := make([]byte, int(util.BtU32(proofsize[:])))
	pFile.Read(proof[:])

	return proof, nil
}

// lookerUpperWorker does the hashing and db read, then returns it's result
// via a channel
func dbLookUp(
	txid string,
	txIndex, blockIndex, height, lookahead int32,
	addChan chan ttl.DeathInfo, db *leveldb.DB) {

	// build string and hash it (nice that this in parallel too)
	utxostring := fmt.Sprintf("%s:%d", txid, txIndex)
	opHash := util.HashFromString(utxostring)

	// make DB lookup
	ttlbytes, err := db.Get(opHash[:], nil)
	if err == leveldb.ErrNotFound {
		ttlbytes = make([]byte, 4) // not found is 0
	} else if err != nil {
		// some other error
		panic(err)
	}
	if len(ttlbytes) != 4 {
		fmt.Printf("val len %d, op %s:%d\n", len(ttlbytes), txid, txIndex)
		panic("ded")
	}

	addChan <- ttl.DeathInfo{DeathHeight: util.BtI32(ttlbytes),
		TxPos: int32(txIndex), Txid: opHash}
	return
}
