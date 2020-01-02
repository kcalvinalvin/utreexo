package ibdsim

import (
	"fmt"

	"github.com/mit-dci/utreexo/cmd/utils"
	"github.com/syndtr/goleveldb/leveldb"
)

type deathInfo struct {
	deathHeight, blockPos, txPos uint32
}

// for each block, make a slice of txotxs in order.  The slice will stay in order.
// also make the deathheights slices for all the txotxs the right size.
// then hand the []txotx slice over to the worker function which can make the
// lookups in parallel and populate the deathheights.  From there you can go
// back to serial to write back to the txofile.

// ttlLookup takes the slice of txotxs and fills in the deathheights
func LookupBlock(block []*simutil.Txotx, db *leveldb.DB) {

	// I don't think buffering this will do anything..?
	infoChan := make(chan deathInfo)

	var remaining uint32

	// go through every tx
	for blockPos, tx := range block {
		// go through every output
		for txPos, _ := range tx.DeathHeights {
			// increment counter, and send off to a worker
			remaining++
			go LookerUpperWorker(
				tx.Outputtxid, uint32(blockPos), uint32(txPos), infoChan, db)
		}
	}

	var rcv deathInfo
	for remaining > 0 {
		//		fmt.Printf("%d left\t", remaining)
		rcv = <-infoChan
		block[rcv.blockPos].DeathHeights[rcv.txPos] = rcv.deathHeight
		remaining--
	}

	return
}

// lookerUpperWorker does the hashing and db read, then returns it's result
// via a channel
func LookerUpperWorker(
	txid string, blockPos, txPos uint32,
	infoChan chan deathInfo, db *leveldb.DB) {

	// start deathInfo struct to send back
	var di deathInfo
	di.blockPos, di.txPos = blockPos, txPos

	// build string and hash it (nice that this in parallel too)
	utxostring := fmt.Sprintf("%s;%d", txid, txPos)
	opHash := simutil.HashFromString(utxostring)

	// make DB lookup
	//fmt.Printf("%x\n", opHash)
	ttlbytes, err := db.Get(opHash[:], nil)
	//fmt.Println("ttlbytes:", ttlbytes)
	if err == leveldb.ErrNotFound {
		//		fmt.Printf("can't find %s;%d in file", txid, txPos)
		ttlbytes = make([]byte, 4) // not found is 0
	} else if err != nil {
		// some other error
		panic(err)
	}
	if len(ttlbytes) != 4 {
		fmt.Printf("val len %d, op %s;%d\n", len(ttlbytes), txid, txPos)
		panic("ded")
	}

	di.deathHeight = simutil.BtU32(ttlbytes)
	//fmt.Println(di.deathHeight)
	// send back to the channel and this output is done
	infoChan <- di

	return
}
