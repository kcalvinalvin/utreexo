package ttl

import (
	"fmt"
	"sync"

	"github.com/mit-dci/utreexo/cmd/util"
	"github.com/syndtr/goleveldb/leveldb"
)

type DeathInfo struct {
	DeathHeight, BlockPos, TxPos int32
	Txid                         [32]byte
}

// WriteBlock sends off ttl info to dbWorker to be written to ttldb
func WriteBlock(txs util.TxToWrite,
	batchan chan *leveldb.Batch, wg *sync.WaitGroup) {

	blockBatch := new(leveldb.Batch)

	for blockindex, tx := range txs.Txs {
		for _, in := range tx.MsgTx().TxIn {
			if blockindex > 0 { // skip coinbase "spend"
				//hashing because blockbatch wants a byte slice
				//TODO Maybe don't convert to a string?
				//Perhaps converting to bytes can work?
				opString := in.PreviousOutPoint.String()
				h := util.HashFromString(opString)
				blockBatch.Put(h[:], util.U32tB(uint32(txs.Height+1)))
			}
		}
	}

	wg.Add(1)

	// send to dbworker to be written to ttldb asynchronously
	batchan <- blockBatch
}

// dbWorker writes everything to the db. It's it's own goroutine so it
// can work at the same time that the reads are happening
// receives from WriteBlock
func DbWorker(
	bChan chan *leveldb.Batch, lvdb *leveldb.DB, wg *sync.WaitGroup) {

	for {
		b := <-bChan
		err := lvdb.Write(b, nil)
		if err != nil {
			fmt.Println(err.Error())
		}
		wg.Done()
	}
}
