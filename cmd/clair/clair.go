package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

/* idea here:
input: load a txo / ttl file, and a memory size
output: write a bitmap of which txos to remember

how to do this:
load everything into a sorted slice (sorted by end time)
every block, remove the beginning of the slice (stuff that has died)
	- flag these as memorable; they made it to the end
add (interspersed) the new txos in the block
chop off the end of the slice (all that exceeds memory capacity)
that's all.

format of the schedule.clr file: bitmaps of 8 txos per byte.  1s mean remember, 0s mean
forget.  Not padded or anything.

format of index file: 4 bytes per block.  *Txo* position of block start, in unsigned
big endian.

So to get from a block height to a txo position, seek to 4*height in the index,
read 4 bytes, then seek to *that* /8 in the schedule file, and shift around as needed.

*/

type txoEnd struct {
	txoIdx uint32 // which utxo (in order)
	end    uint32 // when it dies (block height)
}

type sortableTxoSlice []txoEnd

func (s sortableTxoSlice) Len() int      { return len(s) }
func (s sortableTxoSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s sortableTxoSlice) Less(i, j int) bool {
	return s[i].end < s[j].end
}

func (s *sortableTxoSlice) MergeSort(a sortableTxoSlice) {
	*s = append(*s, a...)
	sort.Sort(s)
}

// assumes a sorted slice.  Splits on a "end" value, returns the low slice and
// leaves the higher "end" value sequence in place
func SplitAfter(s sortableTxoSlice, h uint32) (top, bottom sortableTxoSlice) {
	for i, c := range s {
		if c.end > h {
			top = s[0:i]   // return the beginning of the slice
			bottom = s[i:] // chop that part off
			break
		}
	}
	if top == nil {
		bottom = s
	}
	return
}

func main() {
	fmt.Printf("clair - builds clairvoyant caching schedule\n")
	err := clairvoy()
	if err != nil {
		panic(err)
	}
	fmt.Printf("done\n")

}

func Clairvoy() error {
	txofile, err := os.OpenFile("ttl.mainnet.txos", os.O_RDONLY, 0600)
	if err != nil {
		return err
	}

	scheduleSlice := make([]byte, 125000000)

	// scheduleFile, err := os.Create("schedule.clr")
	// if err != nil {
	// 	return err
	// }
	// we should know how many utxos there are before starting this, and allocate
	// (truncate!? weird) that many bits (/8 for bytes)
	// err = scheduleFile.Truncate(125000000) // 12.5MB for testnet (guess)
	// if err != nil {
	// 	return err
	// }

	// the index file will be useful later for ibdsim, if you have a block
	// height and need to know where in the clair schedule you are.
	// indexFile, err := os.Create("index.clr")
	// if err != nil {
	// 	return err
	// }

	defer txofile.Close()
	// defer scheduleFile.Close()

	var sortTime time.Duration
	startTime := time.Now()
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: clair memorysize  (eg ./clair 3000)\n")
	}
	maxmem, err := strconv.Atoi(os.Args[1])
	if err != nil || maxmem == 0 {
		return fmt.Errorf("usage: clair memorysize  (eg ./clair 3000)\n")

	}
	var blockEnds sortableTxoSlice

	var clairSlice, remembers sortableTxoSlice

	var utxoCounter uint32
	var height uint32
	// _, err = indexFile.WriteAt(U32tB(0), 0) // first 0 bytes because blocks start at 1
	// if err != nil {
	// 	return err
	// }

	// Reads block asynchronously from .dat files
	go simutil.BlockReader(bchan, currentOffsetHeight, height, offsetfile)

	for ; height != currentOffsetHeight && stop != true; height++ {

		b := <-bchan

		err = genPollard(b.Txs, b.Height, &totalTXOAdded,
			lookahead, &totalDels, plustime, pFile, pOffsetFile, lvdb, &p)
		if err != nil {
			panic(err)
		}

		height++
		if height%1000 == 0 {
			fmt.Printf("all %.2f sort %.2f ",
				time.Now().Sub(startTime).Seconds(),
				sortTime.Seconds())
			fmt.Printf("h %d txo %d clairSlice %d ",
				height, utxoCounter, len(clairSlice))
			if len(clairSlice) > 0 {
				fmt.Printf("first %d:%d last %d:%d\n",
					clairSlice[0].txoIdx,
					clairSlice[0].end,
					clairSlice[len(clairSlice)-1].txoIdx,
					clairSlice[len(clairSlice)-1].end)
			} else {
				fmt.Printf("\n")
			}
		}
		//if height%10000 == 0 {
		//	fmt.Printf("Block %d %s plus %.2f total %.2f proofnodes %d \n",
		//		height, newForest.Stats(),
		//		plustime.Seconds(), time.Now().Sub(starttime).Seconds(),
		//		totalProofNodes)
		//}

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

	endHeights, err := plusLine(scanner.Text())
	if err != nil {
		return err
	}
	for _, eh := range endHeights {
		if eh != 0 {
			var nxo txoEnd
			nxo.txoIdx = utxoCounter
			utxoCounter++
			nxo.end = height + eh
			blockEnds = append(blockEnds, nxo)
		}
	}

	//		case 'h':

	// txosThisBlock := uint32(len(blockEnds))

	// append & sort
	sortStart := time.Now()
	// presort the smaller slice
	sort.Sort(blockEnds)
	// merge sorted
	clairSlice = mergeSortedSlices(clairSlice, blockEnds)
	sortTime += time.Now().Sub(sortStart)

	// clear blockEnds
	blockEnds = sortableTxoSlice{}

	// chop off the beginning: that's the stuff that's memorable
	preLen := len(clairSlice)
	remembers, clairSlice = SplitAfter(clairSlice, height)
	postLen := len(clairSlice)
	if preLen != len(remembers)+postLen {
		return fmt.Errorf("h %d preLen %d remembers %d postlen %d\n",
			height, preLen, len(remembers), postLen)
	}

	// chop off the end, that's stuff that is forgettable
	if len(clairSlice) > maxmem {
		//				forgets := clairSlice[maxmem:]
		// fmt.Printf("\tblock %d forget %d\n",
		// height, len(clairSlice)-maxmem)
		clairSlice = clairSlice[:maxmem]

		//				for _, f := range forgets {
		//					fmt.Printf("%d ", f.txoIdx)
		//				}
		//				fmt.Printf("\n")
	}

	// expand index file and schedule file (with lots of 0s)
	// _, err := indexFile.WriteAt(
	// 	U32tB(utxoCounter-txosThisBlock), int64(height)*4)
	// if err != nil {
	// 	return err
	// }

	// writing remembers is trickier; check in
	if len(remembers) > 0 {
		for _, r := range remembers {
			assertBitInRam(r.txoIdx, scheduleSlice)
			// err = assertBitInFile(r.txoIdx, scheduleFile)
			// if err != nil {
			// 	fmt.Printf("assertBitInFile error\n")
			// 	return err
			// }
		}

	}

	// return nil
	fileString := fmt.Sprintf("schedule%dpos.clr", maxmem)
	return ioutil.WriteFile(fileString, scheduleSlice, 0644)

}

// basically flips bit n of a big file to 1.
func assertBitInFile(txoIdx uint32, scheduleFile *os.File) error {
	offset := int64(txoIdx / 8)
	b := make([]byte, 1)
	_, err := scheduleFile.ReadAt(b, offset)
	if err != nil {
		return err
	}
	b[0] = b[0] | 1<<(7-(txoIdx%8))
	_, err = scheduleFile.WriteAt(b, offset)
	return err
}

// flips a bit to 1.  Crashes if you're out of range.
func assertBitInRam(txoIdx uint32, scheduleSlice []byte) {
	offset := int64(txoIdx / 8)
	scheduleSlice[offset] |= 1 << (7 - (txoIdx % 8))
}

// like the plusline in ibdsim.  Should merge with that.
// this one only returns a slice of the expiry times for the txos, but no other
// txo info.
func plusLine(s string) ([]uint32, error) {
	//	fmt.Printf("%s\n", s)
	parts := strings.Split(s[1:], ";")
	if len(parts) < 2 {
		return nil, fmt.Errorf("line %s has no ; in it", s)
	}
	postsemicolon := parts[1]

	indicatorHalves := strings.Split(postsemicolon, "x")
	ttldata := indicatorHalves[1]
	ttlascii := strings.Split(ttldata, ",")
	// the last one is always empty as there's a trailing ,
	ttlval := make([]uint32, len(ttlascii)-1)
	for i, _ := range ttlval {
		if ttlascii[i] == "s" {
			//	ttlval[i] = 0
			// 0 means don't remember it! so 1 million blocks later
			ttlval[i] = 1 << 30
			continue
		}

		val, err := strconv.Atoi(ttlascii[i])
		if err != nil {
			return nil, err
		}
		ttlval[i] = uint32(val)
	}

	txoIndicators := strings.Split(indicatorHalves[0], "z")

	numoutputs, err := strconv.Atoi(txoIndicators[0])
	if err != nil {
		return nil, err
	}
	if numoutputs != len(ttlval) {
		return nil, fmt.Errorf("%d outputs but %d ttl indicators",
			numoutputs, len(ttlval))
	}

	// numoutputs++ // for testnet3.txos

	unspend := make(map[int]bool)

	if len(txoIndicators) > 1 {
		unspendables := txoIndicators[1:]
		for _, zstring := range unspendables {
			n, err := strconv.Atoi(zstring)
			if err != nil {
				return nil, err
			}
			unspend[n] = true
		}
	}
	var ends []uint32
	for i := 0; i < numoutputs; i++ {
		if unspend[i] {
			continue
		}
		ends = append(ends, ttlval[i])
		// fmt.Printf("expire in\t%d remember %v\n", ttlval[i], addData.Remember)
	}

	return ends, nil
}

//Here we write proofs for all the txs.
//All the inputs are saved as 32byte sha256 hashes.
//All the outputs are saved as LeafTXO type.
func genPollard(
	tx []*wire.MsgTx,
	height int,
	lookahead int32,
	totalDels *int,
	plustime time.Duration,
	lvdb *leveldb.DB) error {

	var blockAdds []utreexo.LeafTXO
	blocktxs := []*simutil.Txotx{new(simutil.Txotx)}
	plusstart := time.Now()

	for _, tx := range tx {
		//creates all txos up to index indicated
		txhash := tx.TxHash()
		//fmt.Println(txhash.String())
		numoutputs := uint32(len(tx.TxOut))

		blocktxs[len(blocktxs)-1].Unspendable = make([]bool, numoutputs)
		//Adds z and index for all OP_RETURN transactions
		//We don't keep track of the OP_RETURNS so probably can get rid of this
		for i, out := range tx.TxOut {
			if simutil.IsUnspendable(out) {
				// Skip all the unspendables
				blocktxs[len(blocktxs)-1].Unspendable[i] = true
			} else {
				//txid := tx.TxHash().String()
				blocktxs[len(blocktxs)-1].Outputtxid = txhash.String()
				blocktxs[len(blocktxs)-1].DeathHeights = make([]uint32, numoutputs)
			}
		}

		// done with this Txotx, make the next one and append
		blocktxs = append(blocktxs, new(simutil.Txotx))

	}
	//TODO Get rid of this. This eats up cpu
	//we started a tx but shouldn't have
	blocktxs = blocktxs[:len(blocktxs)-1]
	// call function to make all the db lookups and find deathheights
	lookupBlock(blocktxs, lvdb)

	for _, blocktx := range blocktxs {
		adds, err := genLeafTXO(blocktx, uint32(height+1))
		if err != nil {
			return err
		}
		for _, a := range adds {

			if a.Duration == 0 {
				continue
			}
			//fmt.Println("lookahead: ", lookahead)
			a.Remember = a.Duration < lookahead
			//fmt.Println("Remember", a.Remember)

			*totalTXOAdded++

			blockAdds = append(blockAdds, a)
			//fmt.Println("adds:", blockAdds)
		}
	}
	donetime := time.Now()
	plustime += donetime.Sub(plusstart)
	bpBytes, err := getProof(uint32(height), pFile, pOffsetFile)
	if err != nil {
		return err
	}
	bp, err := utreexo.FromBytesBlockProof(bpBytes)
	if err != nil {
		return err
	}
	if len(bp.Targets) > 0 {
		fmt.Printf("block proof for block %d targets: %v\n", height+1, bp.Targets)
	}
	err = p.IngestBlockProof(bp)
	if err != nil {
		return err
	}

	// totalTXOAdded += len(blockAdds)
	*totalDels += len(bp.Targets)

	err = p.Modify(blockAdds, bp.Targets)
	if err != nil {
		return err
	}
	return nil
}

// plusLine reads in a line of text, generates a utxo leaf, and determines
// if this is a leaf to remember or not.
func genTxoEndHeight(tx *simutil.Txotx, blockEnds *sortableTxoSlice) {
	for i := 0; i < len(tx.DeathHeights); i++ {
		if tx.Unspendable[i] == true {
			continue
		}
		end := txoEnd{
			txoIdx: utreexo.HashFromString(utxostring),
			end:    tx.DeathHeights[i],
		}
		blockEnds = append(blockEnds, endData)
	}
}

// This is copied from utreexo utils, and in this cases there will be no
// duplicates, so that part is removed.  Uses sortableTxoSlices.

// mergeSortedSlices takes two slices (of uint64s; though this seems
// genericizable in that it's just < and > operators) and merges them into
// a signle sorted slice, discarding duplicates.
// (eg [1, 5, 8, 9], [2, 3, 4, 5, 6] -> [1, 2, 3, 4, 5, 6, 8, 9]
func mergeSortedSlices(a sortableTxoSlice, b sortableTxoSlice) (c sortableTxoSlice) {
	maxa := len(a)
	maxb := len(b)

	// make it the right size (no dupes)
	c = make(sortableTxoSlice, maxa+maxb)

	idxa, idxb := 0, 0
	for j := 0; j < len(c); j++ {
		// if we're out of a or b, just use the remainder of the other one
		if idxa >= maxa {
			// a is done, copy remainder of b
			j += copy(c[j:], b[idxb:])
			c = c[:j] // truncate empty section of c
			break
		}
		if idxb >= maxb {
			// b is done, copy remainder of a
			j += copy(c[j:], a[idxa:])
			c = c[:j] // truncate empty section of c
			break
		}

		obja, objb := a[idxa], b[idxb]
		if obja.end < objb.end { // a is less so append that
			c[j] = obja
			idxa++
		} else { // b is less so append that
			c[j] = objb
			idxb++
		}
	}
	return
}
