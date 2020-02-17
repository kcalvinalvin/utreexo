package ibdsim

import (
	"fmt"
	"os"
	"time"

	"github.com/mit-dci/utreexo/cmd/simutil"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// run IBD from block proof data
// we get the new utxo info from the same txos text file
func IBDClient(isTestnet bool,
	offsetfile string, ttldb string, sig chan bool) error {

	//Channel to alert the main loop to break
	stopGoing := make(chan bool, 1)

	//Channel to alert stopTxottl it's ok to exit
	done := make(chan bool, 1)

	go stopRunIBD(sig, stopGoing, done)

	//Check if the ttlfn given is a testnet file
	simutil.CheckTestnet(isTestnet)

	// open database
	o := new(opt.Options)
	o.CompactionTableSizeMultiplier = 8
	o.ReadOnly = true
	lvdb, err := leveldb.OpenFile(ttldb, o)
	if err != nil {
		panic(err)
	}
	defer lvdb.Close()

	simutil.MakePaths()

	// Bool to see if pollarddata is there
	pollardInitialized := simutil.HasAccess(simutil.PollardFilePath)
	currentOffsetHeight, height, p := InitIBDsim(pollardInitialized)

	lookahead := int32(1000) // keep txos that last less than this many blocks

	totalTXOAdded := 0
	totalDels := 0

	//bool for stopping the scanner.Scan loop
	var stop bool

	// To send/receive blocks from blockreader()
	bchan := make(chan simutil.BlockToWrite, 10)

	// Reads blocks asynchronously from blk*.dat files
	go simutil.BlockReader(bchan,
		currentOffsetHeight, height, simutil.OffsetFilePath)

	pFile, err := os.OpenFile(
		simutil.PFilePath, os.O_RDONLY, 0400)
	if err != nil {
		return err
	}

	pOffsetFile, err := os.OpenFile(
		simutil.POffsetFilePath, os.O_RDONLY, 0400)
	if err != nil {
		return err
	}

	var plustime time.Duration
	starttime := time.Now()

	for ; height != currentOffsetHeight && stop != true; height++ {

		b := <-bchan

		err = genPollard(b.Txs, b.Height, &totalTXOAdded,
			lookahead, &totalDels, plustime, pFile, pOffsetFile, lvdb, &p)
		if err != nil {
			panic(err)
		}

		//if height%10000 == 0 {
		//	fmt.Printf("Block %d %s plus %.2f total %.2f proofnodes %d \n",
		//		height, newForest.Stats(),
		//		plustime.Seconds(), time.Now().Sub(starttime).Seconds(),
		//		totalProofNodes)
		//}

		if height%10000 == 0 {
			fmt.Printf("Block %d add %d del %d %s plus %.2f total %.2f \n",
				height+1, totalTXOAdded, totalDels, p.Stats(),
				plustime.Seconds(), time.Now().Sub(starttime).Seconds())
		}
		/*
			if height%100000 == 0 {
				fmt.Printf(MemStatString(fname))
			}
		*/

		//Check if stopSig is no longer false
		//stop = true makes the loop exit
		select {
		case stop = <-stopGoing:
		default:
		}
	}
	pFile.Close()
	pOffsetFile.Close()

	fmt.Printf("Block %d add %d del %d %s plus %.2f total %.2f \n",
		height, totalTXOAdded, totalDels, p.Stats(),
		plustime.Seconds(), time.Now().Sub(starttime).Seconds())

	saveIBDsimData(height, p)

	fmt.Println("Done Writing")

	done <- true

	return nil
}
